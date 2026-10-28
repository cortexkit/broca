//! The WAL writer thread + the loop's barrier handle.
//!
//! The loop NEVER fsyncs inline and NEVER blocks the token path (it only ever
//! WALs ASSEMBLED units, never per-token deltas). It hands records to a writer
//! running on a dedicated OS thread — so idle writers and blocking fsyncs never
//! occupy Tokio's shared blocking pool — and awaits ONLY at the barrier points:
//!  - `record(..)` — fire-and-forget; coalesced into the next fsync.
//!  - `sync().await` — fsync everything pending + await the ack (a barrier).
//!  - `shutdown().await` — drain + final fsync IF DIRTY + join, BEFORE the lease releases.
//!    The writer must not outlive the lease: otherwise a superseded writer could
//!    still append after a new run has acquired the session.
//!
//! The writer is the single owner of the [`Wal`]; the lease epoch is stamped on every append
//! as forensic tenure metadata, not checked as an append gate.

use broca_diagnostics::{spawn_blocking, JobKind};

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};

use broca_protocol::tags::MessageTags;
use broca_protocol::BindIdentity;
use broca_wal::{
    FileWal, Wal, WalByteAnchor, WalError, WalRangeRead, WalRecord, DEFAULT_MAX_READ_BYTES,
};
use tokio::sync::{broadcast, oneshot};

use crate::lease::{LeaseHandle, OwnedWalTenure};

mod extraction;
#[path = "open_writers.rs"]
mod open_writers;
#[cfg(feature = "test-clock")]
thread_local! {
    static WRITER_FS_FOR_TEST: std::cell::RefCell<Option<Arc<dyn broca_wal::fs_ops::FsOps>>> = const { std::cell::RefCell::new(None) };
}
mod projection_reader;
mod retention_changes;
pub use projection_reader::ReadOnlyWalReader;
mod projection_gap;
pub use projection_gap::{
    session_projection_incomplete, ProjectionAdmission, ProjectionRepair,
    SESSION_PROJECTION_INCOMPLETE,
};
mod session_index;
pub use session_index::{agent_session_ref, legacy_session_ref, SessionRefIndex};

/// A writer opened under a session lease, and the lease its caller keeps.
pub type WriterAndLease = (Box<dyn Wal + Send>, Box<dyn LeaseHandle>);

/// Opens the per-session WAL (mirrors `LeaseStore`, so the file impl swaps for a
/// future store without touching the loop).
pub trait WalStore: Send + Sync {
    /// State-root location for the writer-id/v1 marker protocol. Legacy stores need none.
    fn writer_marker_root(&self) -> Option<PathBuf> {
        None
    }
    /// File-backed sessions stage uploads beside their WAL trees, never in a
    /// second host media store. In-memory stores do not offer upload staging.
    fn uploads(&self) -> Option<crate::media_uploads::Uploads> {
        None
    }
    fn backup_classes(&self) -> Vec<String> {
        vec!["head".into()]
    }
    /// Move admission is checked even for cached projections and already-open actors.
    fn check_move(&self, _session: &BindIdentity, _write: bool) -> Result<(), WalError> {
        Ok(())
    }
    /// The frame format the logs this store opens write, decided once at boot. Each log the
    /// store opens reports the same value through [`Wal::wal_config`], which is what a writer
    /// handle reads at spawn. The default, for stores with no state root, is legacy.
    fn wal_config(&self) -> broca_wal::WalConfig {
        broca_wal::WalConfig::LEGACY
    }

    /// Open (creating if absent) the WAL for `session`, ready for appends.
    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError>;

    /// Admission chooses retention only for a fresh lineage, under its lease.
    /// Stores without ephemeral support must refuse rather than persist those
    /// bytes in a portable WAL by accident.
    fn open_with_retention(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        if retention.is_some() {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "WAL store has no ephemeral directory",
            )));
        }
        self.open(session)
    }

    /// The caller holds the session's exclusive writer lease while choosing
    /// first-send placement. Read-only resolution must never choose a new tree.
    fn open_with_class(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        _session_class: Option<&str>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        self.open_with_retention(session, retention)
    }

    /// Open a writer for `session` under `lease` and return the lease the caller keeps. A
    /// store may wrap the lease to start work that must wait until the lease is released,
    /// such as retention enrollment. The default opens with `open_with_class` and returns the
    /// lease unchanged.
    fn open_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        lease: Box<dyn LeaseHandle>,
    ) -> Result<WriterAndLease, WalError> {
        Ok((
            self.open_with_class(session, retention, session_class)?,
            lease,
        ))
    }

    /// Open a writer under the caller-owned session tenure, whose live lease is
    /// `lease`. Unlike [`Self::open_writer`], a store may treat `lease` as the
    /// authority for every mutation the open performs before the descriptor exists,
    /// such as restoring an archived log to its live path. The default delegates to
    /// `open_writer`, so a store without owned-open support keeps its guarded open.
    fn open_owned_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        lease: Box<dyn LeaseHandle>,
    ) -> Result<WriterAndLease, WalError> {
        self.open_writer(session, retention, session_class, lease)
    }

    /// The lowest runner tag a new lineage at `session` may use. When retention deleted an
    /// earlier lineage at this address, its tombstone recorded the tag that lineage would have
    /// given next, and a new lineage must start there so it reuses none of the deleted tags.
    /// With no such tombstone the answer is 1. The default, for a store without tombstones, is 1.
    fn retired_tag_floor(&self, _session: &BindIdentity) -> Result<u64, WalError> {
        Ok(1)
    }

    /// The lowest compaction notice sequence number a new lineage at `session` may use. A
    /// notice's bus id and the provider's duplicate check both key on the session, not the
    /// lineage, so a lineage created after retention deleted an earlier one must number its
    /// notices on from where that one stopped, as its tombstone recorded. With no such
    /// tombstone the answer is 1. The default, for a store without tombstones, is 1.
    fn retired_notice_floor(&self, _session: &BindIdentity) -> Result<u64, WalError> {
        Ok(1)
    }

    /// Read the first run's frozen policy, independently of any materialized watermark.
    fn is_retention_lineage(&self, session: &BindIdentity) -> Result<bool, WalError> {
        self.read_records(session)
            .map(|records| crate::is_retention_lineage(&records))
    }

    /// Whether admission must treat the address as an existing lineage, including
    /// a materialized-only forever lineage. Unknown is an error, not permission
    /// to create a new retention lineage over content the store could not inspect.
    fn lineage_exists(
        &self,
        _session: &BindIdentity,
        _meta: Option<&crate::AdmissionStoreSnapshot>,
    ) -> Result<bool, WalError> {
        Err(WalError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "WAL store cannot resolve lineage existence",
        )))
    }

    /// Whether this session's log EXISTS (live or archived), separately from whether it holds records.
    ///
    /// `None` means the implementation cannot tell, and that is the DEFAULT ON PURPOSE. A
    /// store that does not know must not answer `false`: absence and ignorance send a caller
    /// to different places, and collapsing them would make every in-memory test double claim
    /// authoritatively that no session has a log.
    ///
    /// Test stores may not know presence. File stores report an absent log as
    /// `WalError::Io(NotFound)`; an empty vector means a present, empty log.
    /// Use this probe when a diagnosis needs presence without decoding records.
    fn wal_present(&self, _session: &BindIdentity) -> Option<bool> {
        None
    }

    /// When the session's LIVE log was last written, without consulting any archive.
    ///
    /// `Ok(None)` means there is no live log: the session was folded into an archive (or never
    /// wrote). Only a live log can hold a run a crashed process left unfinished, because a
    /// session is folded only after it has gone quiet, so the startup crash-orphan pass uses
    /// this to skip archived sessions without extracting them.
    ///
    /// The default is `Err(Unsupported)`, not `Ok(None)`: a store that cannot tell must not
    /// claim the log is absent, for the same reason [`Self::wal_present`] defaults to `None`.
    fn live_wal_last_write(
        &self,
        _session: &BindIdentity,
    ) -> Result<Option<std::time::SystemTime>, WalError> {
        Err(WalError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "WAL store cannot report when a live log was last written",
        )))
    }

    /// Size of the live log without decoding records. Unknown or archived logs return
    /// `None`, so status can use its existing actor path instead of an unbounded read.
    fn live_wal_bytes(&self, _session: &BindIdentity) -> Result<Option<u64>, WalError> {
        Ok(None)
    }

    /// A strictly bounded live-file suffix, with no full-read or archive fallback.
    /// Stores without this primitive report unknown rather than decode full history.
    fn read_records_tail_bounded(
        &self,
        _session: &BindIdentity,
        _tail_bytes: u64,
    ) -> Result<Option<WalRangeRead>, WalError> {
        Ok(None)
    }

    /// The session's first durable user message (the first user message of the first
    /// `RunStarted` or `Steered` record that adds one), with the number of WAL bytes read to
    /// find it. A file store reads only as far as that record; this default decodes the whole
    /// log and reports the bytes as unknown (zero). `Ok((None, _))` when the session has no
    /// user message yet or no log.
    fn first_user_message(
        &self,
        session: &BindIdentity,
    ) -> Result<(Option<broca_protocol::Message>, u64), WalError> {
        match self.read_records(session) {
            Ok(records) => Ok((
                records
                    .iter()
                    .find_map(|frame| broca_wal::file_wal::first_user_message_in(&frame.record)),
                0,
            )),
            Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok((None, 0)),
            Err(error) => Err(error),
        }
    }

    /// Read the session's current durable records WITHOUT taking the append
    /// handle — for the persister to project fsynced turns mid-run. Safe to call
    /// while the writer holds its own handle (a torn final record is discarded by
    /// replay; only committed records are returned). File stores return `Io(NotFound)`
    /// for an absent WAL, including after archive lookup, rather than an empty vector.
    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, WalError>;

    /// Read records with their log-header identity from the same snapshot.
    /// Stores without a header retain their existing full-read behavior and
    /// report an unknown identity rather than inventing one from the bind key.
    fn read_records_with_lineage(
        &self,
        session: &BindIdentity,
    ) -> Result<
        (
            Vec<broca_wal::FramedRecord>,
            Option<broca_wal::WalLineageId>,
        ),
        WalError,
    > {
        self.read_records(session).map(|records| (records, None))
    }

    /// Read records after a sequence watermark, optionally bounded by the caller's
    /// last fsynced safe head. Implementations may use `anchor` as a validated byte-offset
    /// hint; correctness must not depend on it.
    ///
    /// The default keeps non-file test stores source-compatible while enforcing the same
    /// sequence bounds over their full read.
    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        _anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        let records = self
            .read_records(session)?
            .into_iter()
            .filter(|record| {
                record.seq > after_seq && through_seq.is_none_or(|head| record.seq <= head)
            })
            .collect();
        Ok(WalRangeRead::without_anchors(records))
    }

    /// Read ownership history before deciding whether a caller may learn migration details.
    /// File stores bypass only the move fence here, not WAL validation or durability proofs.
    /// Callers must use these records solely for authorization and check move admission before
    /// serving session data or executing an operation.
    fn read_records_range_for_authorization(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.read_records_range(session, after_seq, through_seq, anchor)
    }

    /// Read a suffix only when `anchor` is valid. Live-file implementations avoid scanning
    /// from byte zero; immutable archive members must first verify their whole digest.
    /// Implementations that cannot prove the byte offset return `None`, which lets callers use
    /// their existing full-replay path without changing correctness or error behavior.
    fn read_records_range_from_valid_anchor(
        &self,
        _session: &BindIdentity,
        _after_seq: u64,
        _through_seq: Option<u64>,
        _anchor: &WalByteAnchor,
    ) -> Result<Option<WalRangeRead>, WalError> {
        Ok(None)
    }

    /// Flush the WAL through a dedicated read-side handle. Implementations must never use a
    /// writer-recovery open here: readers may run concurrently with the writer and must not
    /// truncate or otherwise mutate the WAL's bytes.
    fn flush_for_read(&self, _session: &BindIdentity) -> Result<Option<u64>, WalError> {
        Err(WalError::Io(io::Error::new(
            io::ErrorKind::Unsupported,
            "WAL store does not support a read-side durability barrier",
        )))
    }

    /// Return a stable file identity used to invalidate read-side durability proofs after a
    /// WAL replacement. Stores without a file identity simply cannot reuse cached proofs.
    fn file_identity(&self, _session: &BindIdentity) -> Result<Option<u64>, WalError> {
        Ok(None)
    }

    /// The durability barrier for ownership reads, with the same fence deferral as
    /// `read_records_range_for_authorization`.
    fn flush_for_authorization(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.flush_for_read(session)
    }

    /// The file identity for ownership-read proofs, without disclosing migration status.
    fn file_identity_for_authorization(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<u64>, WalError> {
        self.file_identity(session)
    }
}

/// File-backed WAL store: one append-only log per session under `root`, keyed by a
/// deterministic hash of the session's bind identity so the same session always maps
/// to the same log across processes and restarts.
pub struct FileWalStore {
    root: PathBuf,
    /// The frame format every log this store opens writes, decided once at boot (see
    /// [`FileWalStore::with_config`]). [`FileWalStore::new`] is legacy only.
    config: broca_wal::WalConfig,
    /// Set by [`FileWalStore::new`] when its state root is not plainly legacy: every writable
    /// open then refuses with this error, so a legacy-only store never writes legacy frames
    /// into a root that may be an overhaul root.
    refusal: Option<broca_wal::StateRootError>,
    backup_classes: Vec<String>,
    max_read_bytes: u64,
    session_index: Arc<Mutex<Option<Arc<dyn SessionRefIndex>>>>,
    move_reads: AtomicU64,
    retention_changes: Arc<retention_changes::Changes>,
    durable_observer: Option<Arc<DurableWalObserver>>,
    #[cfg(any(test, feature = "test-clock"))]
    range_reads: AtomicU64,
}

/// Non-I/O notification of a successful WAL barrier, including raw maintenance writers.
/// The callback must retain only the session identity and durable head, not record bodies.
pub type DurableWalObserver =
    dyn Fn(&BindIdentity, u64, Option<broca_wal::WalLineageId>) + Send + Sync;

/// THREE STATES, NOT TWO, and collapsing any pair of them has now caused a defect each.
///
/// `live_present` — the live WAL was there, so no archive was consulted and none should be:
///   live wins. Distinct from a miss because the session's log EXISTS.
/// `consulted` — an archive directory was there and was searched. Only then is a `None`
///   member an AUTHORITATIVE absence that may refuse a read.
/// neither — no archive directory at all, the legacy shape of every tree that has never
///   folded. A `None` member here means "nothing to look in", and treating it as a
///   confirmed miss makes `session.read` refuse for every session in an unfolded tree.
#[derive(Default)]
struct ArchiveLookup {
    durable: bool,
    live_present: bool,
    consulted: bool,
    member: Option<broca_wal::archive::ArchiveMember>,
}

/// Filesystem metadata identity of the archive inventory, used to validate cached
/// expiry facts without retaining WAL record contents.
/// Non-Unix stores cannot supply a stable identity and must not reuse those caches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveIdentity(Vec<ArchiveFileIdentity>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArchiveFileIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    bytes: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

#[cfg(test)]
thread_local! {
    static ARCHIVE_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Diagnostic sidecar stored next to the `wal/` directory.
pub const WAL_MANIFEST_FILENAME: &str = "wal-manifest";

/// The state root holding a store's log directory: its parent, where the `state-format` marker,
/// the feature floor and the sibling log trees live.
fn state_root_of(root: &Path) -> PathBuf {
    match root.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

impl FileWalStore {
    /// Count human prompts when an older episode has no recorded count, bounded by this
    /// store's byte limit and admission's durable head sequence. An earlier count includes
    /// its file identity and end offset, so only subsequently appended records need reading.
    pub fn human_prompt_count(
        &self,
        session: &BindIdentity,
        through_seq: u64,
        base: Option<&broca_wal::file_wal::HumanPromptCount>,
    ) -> Result<broca_wal::file_wal::HumanPromptCount, WalError> {
        self.check_move(session, false)?;
        if through_seq == 0 {
            return Ok(broca_wal::file_wal::HumanPromptCount {
                count: 0,
                through_seq: 0,
                bytes_read: 0,
                anchor: None,
                lineage_id: None,
            });
        }
        if let Some(path) = self.resolve_live_path(session)? {
            return broca_wal::file_wal::read_human_prompt_count(
                path,
                self.max_read_bytes,
                through_seq,
                base,
            );
        }
        let lookup = self.archived_member(session)?;
        if lookup.live_present {
            return Err(WalError::Io(io::Error::other(
                "WAL changed during human prompt history read",
            )));
        }
        let member = lookup.member.ok_or_else(|| {
            WalError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "human prompt history is absent before its durable head",
            ))
        })?;
        broca_wal::file_wal::count_human_prompts_in_bytes(
            &member.payload,
            self.max_read_bytes,
            through_seq,
        )
    }

    /// Participant-only byte snapshots, including the old copy hidden by a moved
    /// tombstone. Archived results are verified member payloads, never containers.
    /// The move driver must hold session exclusion or recheck the snapshot under
    /// it before creating artifacts. Ordinary session readers must not use this
    /// bypass: `read_records_range` enforces moved-session refusal for them.
    pub fn bytes_for_move(&self, session: &BindIdentity) -> Result<Option<Vec<u8>>, WalError> {
        self.move_reads.fetch_add(1, Ordering::Relaxed);
        use std::io::Read;
        if let Some(member) = self.archived_member(session)?.member {
            return Ok(Some(member.payload));
        }
        let Some(path) = self.resolve_live_path(session)? else {
            return Ok(None);
        };
        let file = std::fs::File::open(path).map_err(WalError::Io)?;
        let mut bytes = Vec::new();
        file.take(self.max_read_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(WalError::Io)?;
        if bytes.len() as u64 > self.max_read_bytes {
            return Err(WalError::TooLarge {
                max_bytes: self.max_read_bytes,
                file_size: bytes.len() as u64,
            });
        }
        Ok(Some(bytes))
    }

    /// Drain changed retained addresses and whether overflow requires a recovery walk.
    /// These hints never establish presence, absence, policy, or a deletion deadline.
    pub fn take_retention_changes(&self) -> (Vec<String>, bool) {
        self.retention_changes.take()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn check_move(&self, session: &BindIdentity, write: bool) -> Result<(), WalError> {
        // A cut file may name the session by its folder-based v1 reference (recorded before
        // the v2 switch) or by its v2 reference. Check both in one pass, so a pending release
        // under either reference outranks a moved tombstone for writes.
        let legacy = legacy_session_ref(
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        );
        broca_wal::migration::guard_refs(
            &self.wal_path(session),
            &[&legacy, &agent_session_ref(session)],
            write,
        )
    }

    fn check_authorization_move(&self, session: &BindIdentity) -> Result<(), WalError> {
        match self.check_move(session, false) {
            Err(WalError::SessionMoving { .. } | WalError::SessionMoved { .. }) => Ok(()),
            result => result,
        }
    }
    /// Read a WAL by its filesystem address, with the same live-first archive policy
    /// as session reads. This is read-only, including when the live tail is torn.
    pub fn read_address_records(
        &self,
        address: &str,
    ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        if address.len() != 16 || !address.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid WAL address",
            )));
        }
        let path = self.root.join(format!("{address}.wal"));
        broca_wal::migration::guard(&path, "", false)?;
        match self
            .read_address_range_for_authorization(address, 0, None, None)?
            .into_records()
        {
            broca_wal::WalRangeRecords::Complete(records) => Ok(records),
            broca_wal::WalRangeRecords::Absent => Ok(Vec::new()),
            _ => unreachable!("unanchored read always reports complete records or absence"),
        }
    }

    pub(crate) fn media_store_class(
        &self,
        address: &str,
    ) -> Result<Option<crate::media_store::MediaStoreClass>, WalError> {
        use crate::media_store::MediaStoreClass;
        if let Some(path) = self.resolve_live_address(address)? {
            return Ok(Some(
                if path.parent() == Some(self.durable_root().as_path()) {
                    MediaStoreClass::Durable
                } else if path.parent() == Some(self.ephemeral_root().as_path()) {
                    MediaStoreClass::Ephemeral
                } else {
                    MediaStoreClass::NonDurable
                },
            ));
        }
        let archived = self.archived_member_at_address(address)?;
        Ok(archived.member.map(|_| {
            if archived.durable {
                MediaStoreClass::Durable
            } else {
                MediaStoreClass::NonDurable
            }
        }))
    }

    pub fn addresses(&self) -> Result<std::collections::BTreeSet<String>, WalError> {
        self.address_inventory(false, &|| false)
            .map(|(addresses, _)| addresses)
    }

    /// Participant discovery keeps unreadable locations separate from absence.
    /// Callers must not interpret a missing match as an unheld ref when this
    /// result contains unreadable addresses or containers.
    pub fn move_addresses(
        &self,
    ) -> Result<(std::collections::BTreeSet<String>, Vec<String>), WalError> {
        self.address_inventory(true, &|| false)
    }

    fn address_inventory(
        &self,
        partial: bool,
        cancelled: &impl Fn() -> bool,
    ) -> Result<(std::collections::BTreeSet<String>, Vec<String>), WalError> {
        let mut addresses = std::collections::BTreeSet::new();
        let mut unreadable = Vec::new();
        let mut failure = |name: String, error: WalError| {
            if partial {
                unreadable.push(name);
                Ok(())
            } else {
                Err(error)
            }
        };
        for root in [&self.root, &self.ephemeral_root(), &self.durable_root()] {
            match std::fs::read_dir(root) {
                Ok(entries) => {
                    for entry in entries {
                        if cancelled() {
                            return Err(WalError::Io(io::Error::new(
                                io::ErrorKind::Interrupted,
                                "WAL address enumeration cancelled",
                            )));
                        }
                        let path = match entry {
                            Ok(entry) => entry.path(),
                            Err(error) => {
                                failure(root.display().to_string(), WalError::Io(error))?;
                                continue;
                            }
                        };
                        if path.extension().is_some_and(|extension| extension == "wal") {
                            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                                addresses.insert(stem.to_owned());
                            }
                        }
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => failure(root.display().to_string(), WalError::Io(error))?,
            }
        }
        for address in addresses.clone() {
            if cancelled() {
                return Err(WalError::Io(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "WAL address enumeration cancelled",
                )));
            }
            if let Err(error) = self.resolve_live_address(&address) {
                failure(address.clone(), error)?;
                addresses.remove(&address);
            }
        }
        let paths = match self.archive_containers() {
            Ok(paths) => paths.unwrap_or_default(),
            Err(error) => {
                failure("wal-archive".into(), error)?;
                Vec::new()
            }
        };
        for path in paths {
            if cancelled() {
                return Err(WalError::Io(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "WAL address enumeration cancelled",
                )));
            }
            let members = broca_wal::archive::ArchiveReader::open(&path, self.max_read_bytes)
                .map_err(WalError::Io)
                .and_then(|mut reader| {
                    if reader.state().map_err(WalError::Io)?
                        == broca_wal::archive::ContainerState::Unsealed
                    {
                        return Ok(Vec::new());
                    }
                    reader.rescan().map_err(WalError::Io).map(|entries| {
                        entries
                            .into_iter()
                            .map(|info| info.session_addr)
                            .collect::<Vec<_>>()
                    })
                });
            match members {
                Ok(members) => addresses.extend(members),
                Err(error) => failure(path.display().to_string(), error)?,
            }
        }
        Ok((addresses, unreadable))
    }

    /// A legacy-only store over the log directory `root` (a state root's `wal/`).
    ///
    /// It reads its state root's `state-format` marker once, here. When the marker is absent the
    /// root is legacy and the store writes today's frames. When the root is marked, or its marker
    /// cannot be read or is unknown, the store is poisoned instead: it still reads, but every
    /// writable open refuses with [`broca_wal::StateRootError::OverhaulRootNeedsConfig`]. A root
    /// whose format was decided at boot uses [`Self::with_config`].
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let refusal = broca_wal::state_format::legacy_store_refusal(&state_root_of(&root));
        Self::build(root, broca_wal::WalConfig::LEGACY, refusal)
    }

    /// A store over the log directory `root` whose frame format was decided once at boot by
    /// [`broca_wal::classify_state_root`]. Every log it opens writes in `config`'s format.
    pub fn with_config(root: impl Into<PathBuf>, config: broca_wal::WalConfig) -> Self {
        Self::build(root.into(), config, None)
    }

    fn build(
        root: PathBuf,
        config: broca_wal::WalConfig,
        refusal: Option<broca_wal::StateRootError>,
    ) -> Self {
        Self {
            root,
            config,
            refusal,
            backup_classes: vec!["head".into()],
            max_read_bytes: DEFAULT_MAX_READ_BYTES,
            session_index: Arc::default(),
            move_reads: AtomicU64::new(0),
            retention_changes: Arc::default(),
            durable_observer: None,
            #[cfg(any(test, feature = "test-clock"))]
            range_reads: AtomicU64::new(0),
        }
    }

    /// Non-durable creation path (FNV-1a of the bind identity). Readers must use
    /// [`Self::resolve_live_path`] instead: a live lineage may be durable or ephemeral.
    ///
    /// THE HASH IS THE ADDRESS AND THE TRIPLE INSIDE THE RECORDS IS NOT CHECKED AGAINST IT.
    /// `RunStarted` embeds the bind triple verbatim, but nothing on the read or replay path
    /// compares it to the key the file was opened under, so a WAL copied to a different
    /// triple's hash serves its full transcript under the new key. That is deliberate on two
    /// counts. It is what lets a moved project root be repaired by renaming files rather than
    /// rewriting durable records, and it is what makes a production WAL usable as a private
    /// test fixture: copy it to a hash you own, drive it, delete the copy, source untouched.
    /// The embedded triple is FORENSIC PROVENANCE -- after a re-key it is the only surviving
    /// evidence of where the session was born, since neither the filename nor the index says
    /// so any more. Adding a triple-vs-key check here would look like hardening and would
    /// break both, so it needs to be a decision rather than a tidy-up.
    pub fn wal_path(&self, session: &BindIdentity) -> PathBuf {
        self.root.join(format!("{}.wal", session_addr(session)))
    }

    pub fn with_durable_observer(mut self, observer: Arc<DurableWalObserver>) -> Self {
        self.durable_observer = Some(observer);
        self
    }

    /// The ordinary directory is passed to the constructor for compatibility with
    /// existing deployments. Ephemeral WALs are its sibling in the same state root.
    pub fn ephemeral_root(&self) -> PathBuf {
        self.root
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join("wal-ephemeral")
    }

    pub fn with_backup_classes(mut self, classes: Vec<String>) -> Self {
        self.backup_classes = classes;
        self
    }

    pub fn durable_root(&self) -> PathBuf {
        self.root
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join("wal-durable")
    }

    pub fn durable_wal_path(&self, session: &BindIdentity) -> PathBuf {
        self.durable_root()
            .join(format!("{}.wal", session_addr(session)))
    }

    /// Move installation preserves the tree of a session already present locally.
    /// For a new session, the receiving daemon applies its configured backup classes
    /// to the incoming WAL's first RunStarted label.
    pub fn move_destination(
        &self,
        session: &BindIdentity,
        session_class: Option<&str>,
    ) -> Result<PathBuf, WalError> {
        if let Some(path) = self.resolve_live_path(session)? {
            return Ok(path);
        }
        let archived = self.archived_member(session)?;
        let durable = if archived.member.is_some() {
            archived.durable
        } else {
            session_class
                .is_some_and(|class| self.backup_classes.iter().any(|value| value == class))
        };
        Ok(if durable {
            self.durable_wal_path(session)
        } else {
            self.wal_path(session)
        })
    }

    pub fn ephemeral_wal_path(&self, session: &BindIdentity) -> PathBuf {
        self.ephemeral_root()
            .join(format!("{}.wal", session_addr(session)))
    }

    pub fn deleting_path(
        &self,
        session: &BindIdentity,
        lineage: broca_wal::WalLineageId,
    ) -> PathBuf {
        self.ephemeral_root()
            .join(format!("{}.{lineage}.deleting", session_addr(session)))
    }

    pub fn tombstone_path(&self, session: &BindIdentity, lineage: &str) -> PathBuf {
        self.ephemeral_root()
            .join(format!("{}.expired", session_addr(session)))
            .join(lineage)
    }

    /// Locate the live file without choosing between conflicting lineages. Files
    /// moved to `.deleting` and tombstone directories are never live WAL inputs.
    pub fn resolve_live_path(&self, session: &BindIdentity) -> Result<Option<PathBuf>, WalError> {
        self.resolve_live_address(&session_addr(session))
    }

    fn resolve_live_address(&self, address: &str) -> Result<Option<PathBuf>, WalError> {
        let mut live: Option<PathBuf> = None;
        for root in [&self.root, &self.ephemeral_root(), &self.durable_root()] {
            let path = root.join(format!("{address}.wal"));
            if broca_wal::wal_file_exists(&path)? {
                if let Some(prior) = live {
                    return Err(WalError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "conflicting live WALs: {} and {}",
                            prior.display(),
                            path.display()
                        ),
                    )));
                }
                live = Some(path);
            }
        }
        Ok(live)
    }

    fn read_path(&self, session: &BindIdentity) -> Result<PathBuf, WalError> {
        self.read_address_path(&session_addr(session))
    }

    fn read_address_path(&self, address: &str) -> Result<PathBuf, WalError> {
        Ok(self
            .resolve_live_address(address)?
            .unwrap_or_else(|| self.root.join(format!("{address}.wal"))))
    }

    /// Admission's existence check includes folded forever lineages whose only
    /// remaining representation is a meta row. A tombstone alone leaves the
    /// address free, and a meta row for that tombstoned lineage does not claim it.
    pub fn lineage_exists(
        &self,
        session: &BindIdentity,
        meta: Option<&crate::AdmissionStoreSnapshot>,
    ) -> Result<bool, WalError> {
        let lookup = self.archived_member(session)?;
        if lookup.live_present || lookup.member.is_some() {
            return Ok(true);
        }
        let Some(meta) = meta else {
            return Ok(false);
        };
        match meta.lineage_id.as_deref() {
            Some(lineage) => self
                .tombstone_path(session, lineage)
                .try_exists()
                .map(|exists| !exists)
                .map_err(WalError::Io),
            None => Ok(true), // A legacy meta row is still an existing forever lineage.
        }
    }

    pub fn max_read_bytes(&self) -> u64 {
        self.max_read_bytes
    }

    #[cfg(feature = "test-clock")]
    pub fn with_read_cap_for_test(mut self, max_bytes: u64) -> Self {
        self.max_read_bytes = max_bytes;
        self
    }

    fn append_cap(&self, scope: retained_size::SizeScope) -> Result<u64, WalError> {
        #[cfg(not(feature = "test-clock"))]
        let _ = scope;
        #[cfg(feature = "test-clock")]
        {
            let path = self
                .root
                .parent()
                .unwrap_or(Path::new(""))
                .join(match scope {
                    retained_size::SizeScope::Session => "test-session-wal-cap",
                    retained_size::SizeScope::RetainedSession => "test-retained-wal-cap",
                });
            match std::fs::read_to_string(path) {
                Ok(value) => {
                    return value.trim().parse().map_err(|_| {
                        WalError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid test retained WAL cap",
                        ))
                    })
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(WalError::Io(error)),
            }
        }
        Ok(self.max_read_bytes)
    }

    /// Wrap an open writer's lease so its release records a retention change hint.
    fn release_hint_lease(
        &self,
        session: &BindIdentity,
        lease: Box<dyn LeaseHandle>,
        retained: Arc<AtomicBool>,
    ) -> Box<dyn LeaseHandle> {
        Box::new(retention_changes::ReleaseHintLease {
            inner: Some(lease),
            address: Arc::from(session_addr(session)),
            changes: self.retention_changes.clone(),
            retained,
        })
    }

    /// Choose a directory only when creating a fresh lineage. Admission validates
    /// the policy before calling this under the session lease; existing live and
    /// archived lineages keep their original directory.
    pub fn open_with_retention(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        self.open_with_class(session, retention, None)
    }

    pub fn open_with_class(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        self.open_observed(
            session,
            retention,
            session_class,
            extraction::RestoreAuthority::GuardedStandalone,
        )
        .map(|(wal, _retained)| wal)
    }

    /// Open the writer and also return its shared retained flag. The flag is set when the WAL
    /// lives under the ephemeral root, or once a retained first run places it there.
    /// `authority` is what permits restoring an archived log to the live path.
    fn open_observed(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        authority: extraction::RestoreAuthority<'_>,
    ) -> Result<(Box<dyn Wal + Send>, Arc<AtomicBool>), WalError> {
        if let Some(refusal) = &self.refusal {
            return Err(WalError::StateRoot(refusal.clone()));
        }
        self.check_move(session, true)?;
        let lookup = self.archived_member(session)?;
        let path = if let Some(member) = lookup.member {
            let path = if lookup.durable {
                self.durable_wal_path(session)
            } else {
                self.wal_path(session)
            };
            // Extraction preserves the logical address and payload; session-ref
            // candidates need no location update.
            extraction::restore(&path, &member.payload, authority).map_err(WalError::Io)?;
            let archive = if lookup.durable {
                "wal-archive-durable"
            } else {
                "wal-archive"
            };
            broca_wal::archive::invalidate_restored_member(
                &self
                    .root
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .join(archive),
                &session_addr(session),
            );
            path
        } else if let Some(path) = self.resolve_live_path(session)? {
            path
        } else if retention.is_some() {
            self.ephemeral_wal_path(session)
        } else if session_class
            .is_some_and(|class| self.backup_classes.iter().any(|value| value == class))
        {
            self.durable_wal_path(session)
        } else {
            self.wal_path(session)
        };
        let identity = format!(
            "{}\u{1f}{}\u{1f}{}",
            session.project_root.display(),
            session.harness,
            session.session
        );
        let retained = path == self.ephemeral_wal_path(session);
        let wal = FileWal::open_with_manifest_and_session_ref(
            path.clone(),
            self.manifest_path(),
            session_addr(session),
            identity,
            agent_session_ref(session),
        )?
        .with_recovery_cap(self.max_read_bytes)
        .with_feature_floor(self.feature_floor_path())
        .with_wal_config(self.config)?;
        let retained_policy = if retained {
            let records = wal.replay()?;
            records
                .iter()
                .find_map(|frame| match &frame.record {
                    WalRecord::RunStarted { retention, .. } => Some(retention.is_some()),
                    _ => None,
                })
                .unwrap_or_else(|| {
                    retention.is_some()
                        || records.iter().any(|frame| {
                            !self.config.is_overhaul()
                                || !matches!(
                                    frame.record,
                                    WalRecord::WriterOpened { .. }
                                        | WalRecord::WriterClosed { .. }
                                        | WalRecord::VisibilityLate { .. }
                                )
                        })
                })
        } else {
            false
        };
        let wal = if retained_policy {
            wal.with_append_budget(self.append_cap(retained_size::SizeScope::RetainedSession)?)
        } else {
            wal.with_recovery_cap(self.append_cap(retained_size::SizeScope::Session)?)
        };
        let wal: Box<dyn Wal + Send> = Box::new(
            session_index::AdmissionWal::new(
                wal,
                session_addr(session),
                self.root.parent().unwrap_or(&self.root).to_path_buf(),
                self.session_index.clone(),
                (
                    path,
                    [
                        legacy_session_ref(
                            &session.harness,
                            &session.project_root.to_string_lossy(),
                            &session.session,
                        ),
                        agent_session_ref(session),
                    ],
                ),
            )
            .with_placement(
                self.wal_path(session),
                self.ephemeral_wal_path(session),
                self.durable_wal_path(session),
                self.backup_classes.clone(),
            ),
        );
        let retained = Arc::new(AtomicBool::new(retained));
        Ok((
            Box::new(retention_changes::ObservedWal {
                inner: wal,
                address: Arc::from(session_addr(session)),
                changes: self.retention_changes.clone(),
                retained: retained.clone(),
                session: session.clone(),
                durable_observer: self.durable_observer.clone(),
            }),
            retained,
        ))
    }

    /// Resolve only after live absence: a live WAL is authoritative even when an archive
    /// contains different bytes. Consulting archives first could roll a resumed session back.
    fn archived_member(&self, session: &BindIdentity) -> Result<ArchiveLookup, WalError> {
        self.archived_member_at_address(&session_addr(session))
    }

    fn archived_member_at_address(&self, address: &str) -> Result<ArchiveLookup, WalError> {
        if self.resolve_live_address(address)?.is_some() {
            #[cfg(test)]
            AFTER_LIVE_STAT.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            return Ok(ArchiveLookup {
                live_present: true,
                ..Default::default()
            });
        }
        let Some(paths) = self.archive_containers()? else {
            return Ok(ArchiveLookup::default());
        };
        #[cfg(test)]
        ARCHIVE_LOOKUPS.with(|count| count.set(count.get() + 1));
        let (durable_paths, ordinary_paths): (Vec<_>, Vec<_>) =
            paths.into_iter().partition(|path| {
                path.parent()
                    .and_then(Path::file_name)
                    .is_some_and(|name| name == "wal-archive-durable")
            });
        let ordinary =
            broca_wal::archive::read_newest_member(&ordinary_paths, address, self.max_read_bytes)
                .map_err(WalError::Io)?;
        let durable =
            broca_wal::archive::read_newest_member(&durable_paths, address, self.max_read_bytes)
                .map_err(WalError::Io)?;
        if ordinary.is_some() && durable.is_some() {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "conflicting archived WALs in wal-archive and wal-archive-durable",
            )));
        }
        Ok(ArchiveLookup {
            consulted: true,
            durable: durable.is_some(),
            member: durable.or(ordinary),
            ..Default::default()
        })
    }

    /// Every archive container path, or `None` when no archive directory exists.
    ///
    /// Discovery is off the live hot path. Do not cache it: another process can publish a
    /// newer sealed pass, and a stale list would silently serve an older transcript.
    fn archive_containers(&self) -> Result<Option<Vec<PathBuf>>, WalError> {
        let mut paths = Vec::new();
        let mut exists = false;
        for name in ["wal-archive", "wal-archive-durable"] {
            let directory = self
                .root
                .parent()
                .unwrap_or_else(|| Path::new(""))
                .join(name);
            let entries = match std::fs::read_dir(directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(WalError::Io(error)),
            };
            exists = true;
            for entry in entries {
                let path = entry.map_err(WalError::Io)?.path();
                if path.extension().is_some_and(|extension| extension == "ark") {
                    paths.push(path);
                }
            }
        }
        Ok(exists.then_some(paths))
    }

    /// Expiry facts are reusable only while the archive inventory's metadata identity
    /// is unchanged. Publication, removal and metadata-visible changes invalidate it.
    /// This uses metadata only; it neither opens containers nor reads member payloads.
    pub fn archive_identity(&self) -> Result<Option<ArchiveIdentity>, WalError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let Some(mut paths) = self.archive_containers()? else {
                return Ok(None);
            };
            paths.sort();
            let mut files = Vec::with_capacity(paths.len());
            for path in paths {
                let meta = match std::fs::metadata(&path) {
                    Ok(meta) => meta,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(WalError::Io(e)),
                };
                files.push(ArchiveFileIdentity {
                    path,
                    device: meta.dev(),
                    inode: meta.ino(),
                    bytes: meta.len(),
                    modified: (meta.mtime(), meta.mtime_nsec()),
                    changed: (meta.ctime(), meta.ctime_nsec()),
                });
            }
            Ok(Some(ArchiveIdentity(files)))
        }
        #[cfg(not(unix))]
        Ok(None)
    }

    /// A sealed archive is already durable, so expiry needs no separate flush/read pair.
    /// If no archived member is found, return `None` so the caller follows its normal
    /// live-WAL read path, including the durability flush when needed.
    pub fn read_archived_for_expiry(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<(WalRangeRead, u64)>, WalError> {
        self.check_move(session, false)?;
        self.archived_member(session)?
            .member
            .map(|member| {
                member_range(&member, 0, None).map(|read| (read, member.info.payload_len))
            })
            .transpose()
    }

    /// Build the archive membership cache now, so the first new session after a restart
    /// doesn't pay for it inside admission.
    ///
    /// The first negative lookup builds the process-wide cache. A container whose v2 index
    /// verifies is built from that index alone, one sequential read plus a SHA-256, so its
    /// cost scales with index bytes. Every other container is built by reading every member
    /// header in it. On the 27,693-member production container (a v1 index) that
    /// measured 10,821 ms cold, right after a restart (2026-09-25 18:46Z), against ~150 ms
    /// warm. A new session pays it before its WAL can be created, which exceeded the
    /// client's 10 s send deadline twice. Calling this once at boot, off the send path,
    /// moves the cost there. A send that arrives while it's still running waits for the
    /// same build rather than starting a second one, so it's never worse than before.
    ///
    /// It runs an ordinary lookup for an address the FNV-1a hash cannot produce for any
    /// real session in practice, and ignores the answer. The only effect is the cache.
    /// Returns how many containers were consulted.
    ///
    /// Containers are warmed concurrently, one single-container lookup each, so the boot
    /// window is the slowest container's build rather than the sum of all of them. A new
    /// session asks every container in turn, so it waits for all of them either way.
    /// Each container's build still happens once: the per-container cell in the
    /// process-wide cache is shared by this warm-up and by any session lookup that
    /// arrives meanwhile, which waits for the build in progress instead of starting
    /// another. The newest containers are started first because sessions consult them
    /// first.
    pub fn prewarm_archive_membership(&self) -> Result<usize, WalError> {
        // Each build already checks headers on several threads of its own, so a few
        // containers at a time keep the device busy without flooding the host.
        const CONCURRENT_CONTAINERS: usize = 4;
        let Some(mut paths) = self.archive_containers()? else {
            return Ok(0);
        };
        // Stamps that do not parse sort last; the lookup itself rejects them below.
        paths.sort_by_key(|path| {
            std::cmp::Reverse(broca_wal::archive::fold_timestamp(path).unwrap_or(0))
        });
        let next = std::sync::atomic::AtomicUsize::new(0);
        let first_error = std::sync::Mutex::new(None);
        let warm = || {
            while let Some(path) =
                paths.get(next.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
            {
                if let Err(error) =
                    broca_wal::archive::prewarm_container_membership(path, self.max_read_bytes)
                {
                    first_error
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .get_or_insert(error);
                }
            }
        };
        std::thread::scope(|scope| {
            // The calling thread warms containers too. A helper that fails to spawn only
            // costs speed: the remaining workers still take every container.
            for _ in 1..paths.len().min(CONCURRENT_CONTAINERS) {
                let _ = std::thread::Builder::new()
                    .name("archive-prewarm".into())
                    .spawn_scoped(scope, warm);
            }
            warm();
        });
        if let Some(error) = first_error.into_inner().unwrap_or_else(|e| e.into_inner()) {
            return Err(WalError::Io(error));
        }
        Ok(paths.len())
    }

    // A failed live open can race with both fold and extraction. Keep a confirmed
    // archive miss separate from a restored live path: the latter must be read again,
    // not replaced by the earlier absence result or by potentially older archive bytes.
    // The live retry is bounded; another disappearance must fail rather than lose records.
    fn retry_archived_member<T>(
        &self,
        session: &BindIdentity,
        absent: impl FnOnce(bool) -> T,
        read_member: impl FnOnce(&broca_wal::archive::ArchiveMember) -> Result<T, WalError>,
        read_live: impl FnOnce() -> Result<T, WalError>,
    ) -> Result<T, WalError> {
        self.retry_archived_address(&session_addr(session), absent, read_member, read_live)
    }

    fn retry_archived_address<T>(
        &self,
        address: &str,
        absent: impl FnOnce(bool) -> T,
        read_member: impl FnOnce(&broca_wal::archive::ArchiveMember) -> Result<T, WalError>,
        read_live: impl FnOnce() -> Result<T, WalError>,
    ) -> Result<T, WalError> {
        #[cfg(test)]
        BEFORE_ARCHIVE_RETRY.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let lookup = self.archived_member_at_address(address)?;
        // ASK FOR LIVE PRESENCE DIRECTLY. `!consulted` used to stand in for "the live
        // file came back", which held only while a missing archive directory ALSO set
        // `consulted`. Once that stopped being a confirmed miss, this branch sent every
        // unfolded tree down read_live() for a file that is genuinely gone.
        if let Some(member) = lookup.member {
            read_member(&member)
        } else if lookup.live_present {
            read_live()
        } else {
            // TWO DIFFERENT ABSENCES. A CONSULTED miss is authoritative and may refuse a
            // read; "there was no archive directory" is the shape of every tree that has
            // never folded and must stay the legacy answer.
            Ok(absent(lookup.consulted))
        }
    }

    fn retry_live_flush(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.read_path(session)?)
            .map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        self.retry_live_identity(session)
    }

    fn flush_for_read_with_identity(
        &self,
        session: &BindIdentity,
        read_identity: impl FnOnce(&Path) -> Result<Option<u64>, WalError>,
    ) -> Result<Option<u64>, WalError> {
        self.check_move(session, false)?;
        self.flush_for_authorization_with_identity(session, read_identity)
    }

    fn flush_for_authorization_with_identity(
        &self,
        session: &BindIdentity,
        read_identity: impl FnOnce(&Path) -> Result<Option<u64>, WalError>,
    ) -> Result<Option<u64>, WalError> {
        self.check_authorization_move(session)?;
        if let Some(member) = self.archived_member(session)?.member {
            // A sealed archive was fsynced before publication and its digest just verified.
            // Reads need no write handle or extraction to prove these immutable bytes.
            return Ok(Some(member_identity(&member)));
        }
        // This is deliberately not FileWal::open: recovery-open may truncate a torn tail, while
        // a read-side proof is allowed to fsync only. Write access is required by Windows
        // FlushFileBuffers, and Rust's shared open mode permits this alongside the append writer.
        let path = self.read_path(session)?;
        let file = match std::fs::OpenOptions::new().write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return self.retry_archived_member(
                    session,
                    |_consulted| None,
                    |member| Ok(Some(member_identity(member))),
                    || self.retry_live_flush(session),
                );
            }
            Err(error) => return Err(WalError::Io(error)),
        };
        file.sync_all().map_err(WalError::Io)?;
        let identity = read_identity(&path)?;
        // The open handle has already proved durability. A missing identity means the proof
        // cannot be cached (always the case off Unix), not that another flush is needed.
        // Reopening by pathname could race retention deletion and flush a different lineage
        // or fail after the original WAL has moved to its deleting path.
        Ok(identity)
    }

    fn retry_live_identity(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        // Unlike the public identity helper, a retry must not turn NotFound into None.
        // Platforms without file identities still return None for an existing file.
        let path = self.read_path(session)?;
        std::fs::metadata(&path).map_err(WalError::Io)?;
        let identity = broca_wal::file_identity(path)?;
        if cfg!(unix) && identity.is_none() {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "live WAL disappeared again during identity retry",
            )));
        }
        Ok(identity)
    }

    fn retry_live_address_range(
        &self,
        address: &str,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        let range = broca_wal::read_records_range(
            self.read_address_path(address)?,
            self.max_read_bytes,
            after_seq,
            through_seq,
            anchor,
        )?;
        if matches!(range.records(), broca_wal::WalRangeRecords::Absent) {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "live WAL disappeared again during archive retry",
            )));
        }
        Ok(range)
    }

    fn read_address_range(
        &self,
        address: &str,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        broca_wal::migration::guard(&self.root.join(format!("{address}.wal")), "", false)?;
        self.read_address_range_for_authorization(address, after_seq, through_seq, anchor)
    }

    /// The newest records of a session's WAL, read from at most its last `tail_bytes` when the
    /// WAL is live. An archived lineage is immutable and is read whole from its archive member.
    /// See [`broca_wal::read_records_tail`].
    pub fn read_records_tail(
        &self,
        session: &BindIdentity,
        tail_bytes: u64,
    ) -> Result<broca_wal::WalTailRead, WalError> {
        let address = session_addr(session);
        broca_wal::migration::guard(&self.root.join(format!("{address}.wal")), "", false)?;
        let lookup = self.archived_member_at_address(&address)?;
        let whole = |range: WalRangeRead| {
            let lineage_id = range.lineage_id;
            match range.into_records() {
                broca_wal::WalRangeRecords::Complete(records) => broca_wal::WalTailRead {
                    records,
                    from_start: true,
                    lineage_id,
                    file_present: true,
                },
                _ => broca_wal::WalTailRead {
                    records: Vec::new(),
                    from_start: true,
                    lineage_id: None,
                    file_present: false,
                },
            }
        };
        if let Some(member) = lookup.member {
            return Ok(whole(member_range(&member, 0, None)?));
        }
        if lookup.consulted {
            return Ok(whole(WalRangeRead::absent_after_archive_lookup()));
        }
        broca_wal::read_records_tail(
            self.read_address_path(&address)?,
            self.max_read_bytes,
            tail_bytes,
        )
    }

    fn read_address_range_for_authorization(
        &self,
        address: &str,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        #[cfg(any(test, feature = "test-clock"))]
        self.range_reads.fetch_add(1, Ordering::Relaxed);
        let lookup = self.archived_member_at_address(address)?;
        if let Some(member) = lookup.member {
            return member_range(&member, after_seq, through_seq);
        }
        if lookup.consulted {
            return Ok(WalRangeRead::absent_after_archive_lookup());
        }
        let range = broca_wal::read_records_range(
            self.read_address_path(address)?,
            self.max_read_bytes,
            after_seq,
            through_seq,
            anchor,
        )?;
        if matches!(range.records(), broca_wal::WalRangeRecords::Absent) {
            return self.retry_archived_address(
                address,
                |consulted| {
                    if consulted {
                        WalRangeRead::absent_after_archive_lookup()
                    } else {
                        range
                    }
                },
                |member| member_range(member, after_seq, through_seq),
                || self.retry_live_address_range(address, after_seq, through_seq, anchor),
            );
        }
        Ok(range)
    }

    #[cfg(any(test, feature = "test-clock"))]
    /// Count actual store range requests so polling tests observe accidental replays.
    pub fn range_reads_for_test(&self) -> u64 {
        self.range_reads.load(Ordering::Relaxed)
    }

    /// The WAL feature floor for this tree: `wal-feature-floor` in the state root, next to the
    /// `wal/` directory, which is where `scripts/place-release.sh` reads it.
    pub fn feature_floor_path(&self) -> PathBuf {
        self.root
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(broca_wal::FEATURE_FLOOR_FILENAME)
    }

    /// Path to the diagnostic index for this WAL tree.
    pub fn manifest_path(&self) -> PathBuf {
        self.root
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(WAL_MANIFEST_FILENAME)
    }
}

/// The highest `next_tag` the retention tombstones in `dir` recorded, or 1. A tombstone is the
/// JSON object retention writes when it deletes a lineage; one written before tags existed has
/// no `next_tag` and counts as 1. An unreadable tombstone is an error: guessing low could make a
/// new lineage reuse a deleted lineage's tags.
pub fn retired_tag_floor_in(dir: &Path) -> Result<u64, WalError> {
    retired_floor_in(dir, "next_tag")
}

/// The highest `next_notice_seq` the retention tombstones in `dir` recorded, or 1: the first
/// compaction notice sequence number a new lineage at that address may use. A tombstone written
/// before notices existed has none and counts as 1.
pub fn retired_notice_floor_in(dir: &Path) -> Result<u64, WalError> {
    retired_floor_in(dir, "next_notice_seq")
}

/// The highest value of the number `key` that the tombstones in `dir` recorded, or 1.
fn retired_floor_in(dir: &Path, key: &str) -> Result<u64, WalError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(1),
        Err(error) => return Err(WalError::Io(error)),
    };
    let mut floor = 1;
    for entry in entries {
        let path = entry.map_err(WalError::Io)?.path();
        if !path.is_file() || path.extension().is_some() {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(WalError::Io(error)),
        };
        let stone: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|error| WalError::Io(io::Error::new(io::ErrorKind::InvalidData, error)))?;
        if let Some(next) = stone.get(key).and_then(serde_json::Value::as_u64) {
            floor = floor.max(next);
        }
    }
    Ok(floor)
}

/// The address half of `wal_path`: FNV-1a over the bind triple, rendered as the WAL's stem.
///
/// SINGLE-SITED ON PURPOSE. This was inline in `wal_path` until 2026-09-04, and the reason to
/// extract it rather than recompute it elsewhere is that a second copy of an addressing function
/// is a second thing that must agree: if a diagnostic renders one hash and the writer opens
/// another, an operator following the log lands on the wrong file -- or on no file, and concludes
/// the session does not exist.
///
/// Useful in a log line because it is the ONLY handle that leads to the bytes. A session name
/// alone does not: the WAL is named by the hash of the whole triple, so a reader holding
/// `alfonso:oneshot-7f3` and a directory of 30,000 files has no way to get from one to the other.
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

impl WalStore for FileWalStore {
    fn writer_marker_root(&self) -> Option<PathBuf> {
        self.config
            .is_overhaul()
            .then(|| state_root_of(&self.root).to_path_buf())
    }
    fn wal_config(&self) -> broca_wal::WalConfig {
        self.config
    }
    fn uploads(&self) -> Option<crate::media_uploads::Uploads> {
        self.root.parent().map(crate::media_uploads::Uploads::at)
    }
    fn open_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        lease: Box<dyn LeaseHandle>,
    ) -> Result<WriterAndLease, WalError> {
        let (wal, retained) = self.open_observed(
            session,
            retention,
            session_class,
            extraction::RestoreAuthority::GuardedStandalone,
        )?;
        Ok((wal, self.release_hint_lease(session, lease, retained)))
    }
    fn open_owned_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        lease: Box<dyn LeaseHandle>,
    ) -> Result<WriterAndLease, WalError> {
        // The caller's tenure is the authority for restoring an archived log:
        // extraction checks that this live lease is the session's own before it
        // writes anything, and the same lease then moves on to the writer task.
        let (wal, retained) = self.open_observed(
            session,
            retention,
            session_class,
            extraction::RestoreAuthority::OwnedTenure {
                lease: lease.as_ref(),
                session,
            },
        )?;
        Ok((wal, self.release_hint_lease(session, lease, retained)))
    }
    fn read_records_tail_bounded(
        &self,
        session: &BindIdentity,
        tail_bytes: u64,
    ) -> Result<Option<WalRangeRead>, WalError> {
        self.check_move(session, false)?;
        broca_wal::file_wal::read_records_tail_bounded(
            self.read_path(session)?,
            self.max_read_bytes,
            tail_bytes,
        )
    }
    fn backup_classes(&self) -> Vec<String> {
        self.backup_classes.clone()
    }
    fn retired_tag_floor(&self, session: &BindIdentity) -> Result<u64, WalError> {
        retired_tag_floor_in(&self.tombstone_path(session, ""))
    }
    fn retired_notice_floor(&self, session: &BindIdentity) -> Result<u64, WalError> {
        retired_notice_floor_in(&self.tombstone_path(session, ""))
    }
    fn open_with_class(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        FileWalStore::open_with_class(self, session, retention, session_class)
    }
    fn check_move(&self, session: &BindIdentity, write: bool) -> Result<(), WalError> {
        FileWalStore::check_move(self, session, write)
    }
    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        self.open_with_retention(session, None)
    }

    fn open_with_retention(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        FileWalStore::open_with_retention(self, session, retention)
    }

    fn lineage_exists(
        &self,
        session: &BindIdentity,
        meta: Option<&crate::AdmissionStoreSnapshot>,
    ) -> Result<bool, WalError> {
        FileWalStore::lineage_exists(self, session, meta)
    }

    fn first_user_message(
        &self,
        session: &BindIdentity,
    ) -> Result<(Option<broca_protocol::Message>, u64), WalError> {
        self.check_move(session, false)?;
        // The live log is read only through the record that carries the message, the way
        // retention classification reads only through the first `RunStarted`. The bound fits
        // two maximal records, so a first `RunStarted` without a user message still finds the
        // steer after it; a log needing more fails rather than loading everything.
        if let Some(path) = self.resolve_live_path(session)? {
            let max = 2
                * (broca_wal::framing::MAX_PAYLOAD_LEN as u64
                    + broca_wal::framing::HEADER_LEN as u64)
                + 2 * broca_wal::framing::LINEAGE_FRAME_LEN as u64;
            return broca_wal::file_wal::read_first_user_message(path, max);
        }
        // An archived log is immutable and read through the existing archive path.
        match self.read_records(session) {
            Ok(records) => Ok((
                records
                    .iter()
                    .find_map(|frame| broca_wal::file_wal::first_user_message_in(&frame.record)),
                0,
            )),
            Err(WalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok((None, 0)),
            Err(error) => Err(error),
        }
    }

    fn is_retention_lineage(&self, session: &BindIdentity) -> Result<bool, WalError> {
        self.check_move(session, false)?;
        // Classification reads only the bounded prefix through the first admitted run.
        // Inconclusive oversized prefixes fail closed rather than loading the whole log.
        if let Some(path) = self.resolve_live_path(session)? {
            let max = broca_wal::framing::MAX_PAYLOAD_LEN as u64
                + broca_wal::framing::HEADER_LEN as u64
                + 2 * broca_wal::framing::LINEAGE_FRAME_LEN as u64;
            return broca_wal::file_wal::read_first_run_started(path, max).map(|first| {
                first.is_some_and(|frame| match frame.record {
                    WalRecord::RunStarted { retention, .. } => retention.is_some(),
                    _ => false,
                })
            });
        }
        // Immutable archive reads keep their existing validation path.
        let range = self.read_records_range(session, 0, Some(1), None)?;
        if let broca_wal::WalRangeRecords::Complete(records) = range.records() {
            if records
                .first()
                .is_some_and(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
            {
                return Ok(crate::is_retention_lineage(records));
            }
            if records.is_empty() {
                return Ok(false);
            }
        }
        if matches!(range.records(), broca_wal::WalRangeRecords::Absent) {
            return Ok(false);
        }
        self.read_records(session)
            .map(|records| crate::is_retention_lineage(&records))
    }

    fn wal_present(&self, session: &BindIdentity) -> Option<bool> {
        // ONE OBSERVATION, NOT TWO. This used to stat the live path and then consult the
        // archive, which left a window: an extraction restoring the live file between the
        // two steps produced `Some(false)` -- "authoritatively no log" -- about a log at
        // the canonical path. Its one production caller is the store's control-head repair,
        // which would then file the session as permanently `declined_no_wal`.
        //
        // `archived_member` already makes the live check ITSELF and returns without
        // consulting anything when the live file is there, so asking it once answers both
        // halves from a single observation and the window cannot exist. `consulted == false`
        // means exactly that live-wins return; a missing archive directory is a CONFIRMED
        // miss and sets the flag.
        //
        // An IO failure stays `None` rather than becoming `false`: reporting "no log here"
        // for a log we were merely unable to look at is the conflation this method exists
        // to remove.
        match self.archived_member(session) {
            Ok(lookup) => Some(lookup.member.is_some() || lookup.live_present),
            Err(_) => None,
        }
    }

    fn live_wal_last_write(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<std::time::SystemTime>, WalError> {
        // A stat of the live path only. Archives are deliberately not consulted: a folded
        // session has no live file, and this answer is how a caller learns to leave it folded.
        match std::fs::metadata(self.read_path(session)?) {
            Ok(metadata) => metadata.modified().map(Some).map_err(WalError::Io),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(WalError::Io(error)),
        }
    }

    fn live_wal_bytes(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.resolve_live_path(session)?
            .map(|path| {
                std::fs::metadata(path)
                    .map(|metadata| metadata.len())
                    .map_err(WalError::Io)
            })
            .transpose()
    }

    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.read_records_with_lineage(session)
            .map(|(records, _)| records)
    }

    fn read_records_with_lineage(
        &self,
        session: &BindIdentity,
    ) -> Result<
        (
            Vec<broca_wal::FramedRecord>,
            Option<broca_wal::WalLineageId>,
        ),
        WalError,
    > {
        // READ-ONLY: decode the committed records, discarding a torn final record in
        // memory. This must NOT go through `FileWal::open` — open truncates a torn tail
        // (writer-recovery), and a reader runs CONCURRENTLY with the live writer, so
        // truncating would cut the writer's own in-progress append off its file and trip
        // its single-writer file-integrity check (ForeignWrite). `replay_file` never
        // opens a write handle, so a concurrent attach replay can't corrupt the writer.
        // The range reader distinguishes an absent log from a present empty log and retries
        // archive lookup if the live file moved into an archive during the read. replay_file
        // deliberately collapses NotFound to empty, so it cannot supply that distinction.
        let range = self.read_records_range(session, 0, None, None)?;
        let lineage = range.lineage_id;
        match range.into_records() {
            broca_wal::WalRangeRecords::Complete(records) => Ok((records, lineage)),
            broca_wal::WalRangeRecords::Absent => Err(WalError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "session WAL is absent after live and archive lookup",
            ))),
            _ => unreachable!("file stores report complete or absent ranges"),
        }
    }

    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.read_address_range(&session_addr(session), after_seq, through_seq, anchor)
    }

    fn read_records_range_for_authorization(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.check_authorization_move(session)?;
        self.read_address_range_for_authorization(
            &session_addr(session),
            after_seq,
            through_seq,
            anchor,
        )
    }

    fn read_records_range_from_valid_anchor(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: &WalByteAnchor,
    ) -> Result<Option<WalRangeRead>, WalError> {
        self.check_move(session, false)?;
        if let Some(member) = self.archived_member(session)?.member {
            // Archive reads already verify the whole payload digest. Validate the anchor
            // against those bytes rather than interpreting a former live inode's offset.
            let full = member_range(&member, 0, None)?;
            if anchor.seq != after_seq || full.anchor_at(after_seq).as_ref() != Some(anchor) {
                return Ok(None);
            }
            return member_range(&member, after_seq, through_seq).map(Some);
        }
        let range = broca_wal::read_records_range_from_valid_anchor(
            self.read_path(session)?,
            self.max_read_bytes,
            after_seq,
            through_seq,
            anchor,
        )?;
        if range.is_none() {
            return self.retry_archived_member(
                session,
                |_consulted| None,
                |member| {
                    let full = member_range(member, 0, None)?;
                    if anchor.seq == after_seq && full.anchor_at(after_seq).as_ref() == Some(anchor)
                    {
                        return member_range(member, after_seq, through_seq).map(Some);
                    }
                    Ok(None)
                },
                || {
                    // None rejects an anchor, not the WAL: callers must fall back to full replay.
                    broca_wal::read_records_range_from_valid_anchor(
                        self.read_path(session)?,
                        self.max_read_bytes,
                        after_seq,
                        through_seq,
                        anchor,
                    )
                },
            );
        }
        Ok(range)
    }

    fn flush_for_read(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.flush_for_read_with_identity(session, |path| broca_wal::file_identity(path))
    }

    fn file_identity(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.check_move(session, false)?;
        self.file_identity_for_authorization(session)
    }

    fn flush_for_authorization(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.flush_for_authorization_with_identity(session, |path| broca_wal::file_identity(path))
    }

    fn file_identity_for_authorization(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<u64>, WalError> {
        self.check_authorization_move(session)?;
        if let Some(member) = self.archived_member(session)?.member {
            return Ok(Some(member_identity(&member)));
        }
        let identity = broca_wal::file_identity(self.read_path(session)?)?;
        if identity.is_none() {
            return self.retry_archived_member(
                session,
                |_consulted| None,
                |member| Ok(Some(member_identity(member))),
                || self.retry_live_identity(session),
            );
        }
        Ok(identity)
    }
}

#[cfg(test)]
thread_local! {
    static AFTER_LIVE_STAT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
    static BEFORE_ARCHIVE_RETRY: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
}

// Content identity invalidates cached proofs when a newer archive changes the payload.
// As with live file identities this is a cache hint, not the integrity check (the full
// SHA-256 is verified by read_newest_member before any bytes are served).
fn member_identity(member: &broca_wal::archive::ArchiveMember) -> u64 {
    u64::from_le_bytes(member.info.payload_sha256[..8].try_into().unwrap())
}

fn member_range(
    member: &broca_wal::archive::ArchiveMember,
    after_seq: u64,
    through_seq: Option<u64>,
) -> Result<WalRangeRead, WalError> {
    broca_wal::file_wal::read_verified_member_range(
        &member.payload,
        member_identity(member),
        after_seq,
        through_seq,
    )
}

pub(crate) use broca_wal::size_limit as retained_size;

/// Only accepted enqueue operations have a sequence. A refused append must not be
/// published as an attempt, call key, projection cursor or any other WAL reference.
#[derive(Debug, Clone)]
pub enum RecordOutcome {
    Accepted(u64),
    Refused(WalWriteError),
}

impl RecordOutcome {
    pub fn into_result(self) -> Result<u64, WalWriteError> {
        match self {
            Self::Accepted(seq) => Ok(seq),
            Self::Refused(error) => Err(error),
        }
    }
}

struct AppendBudget {
    writer_close_reserve: u64,
    retained: bool,
    session_max_bytes: u64,
    max_bytes: u64,
    used_bytes: u64,
    overhead: u64,
    reserve: u64,
    refused: bool,
}

impl AppendBudget {
    fn scope(&self) -> retained_size::SizeScope {
        if self.retained {
            retained_size::SizeScope::RetainedSession
        } else {
            retained_size::SizeScope::Session
        }
    }

    fn refusal(&self) -> WalWriteError {
        match self.scope() {
            retained_size::SizeScope::Session => WalWriteError::SessionTooLarge {
                max_bytes: self.max_bytes,
            },
            retained_size::SizeScope::RetainedSession => WalWriteError::RetainedSessionTooLarge {
                max_bytes: self.max_bytes,
            },
        }
    }
}

/// A failure reported by WalHandle::sync while committing queued records to disk.
/// I/O failures are fatal. A byte-budget refusal leaves accepted
/// records writable so the run boundary can append and commit an Error terminal.
#[derive(Debug, Clone)]
pub enum WalWriteError {
    Failed(String),
    ProofPathUnavailable,
    RetainedSessionTooLarge { max_bytes: u64 },
    SessionTooLarge { max_bytes: u64 },
}

impl std::fmt::Display for WalWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(detail) => write!(f, "wal write failure: {detail}"),
            Self::ProofPathUnavailable => write!(
                f,
                "visibility-late/v1: overhaul WAL has no file path for its durable-head proof"
            ),
            Self::SessionTooLarge { max_bytes } => {
                write!(
                    f,
                    "{}",
                    retained_size::scoped_error(*max_bytes, retained_size::SizeScope::Session)
                        .message
                )
            }
            Self::RetainedSessionTooLarge { max_bytes } => {
                write!(f, "{}", retained_size::error(*max_bytes).message)
            }
        }
    }
}

impl std::error::Error for WalWriteError {}

/// Why a session WAL could not be opened, recovered and proven for writing.
///
/// A WAL that a newer build wrote is kept apart from every other failure so the caller can
/// refuse it by name. Its bytes are intact and nothing changes between retries; only a build
/// that knows the missing features can open it. Every other cause keeps the plain
/// [`WalWriteError`] it always had. Both variants render exactly the text a plain
/// [`WalWriteError`] would, so a message or log line built from this error reads as before.
#[derive(Debug, Clone)]
pub enum WalOpenError {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: String,
    },
    /// A complete frame requires WAL features this build does not know (`missing`).
    NewerWriter {
        missing: Vec<String>,
        error: WalWriteError,
    },
    /// Any other open, recovery or proof failure.
    Failed(WalWriteError),
}

impl WalOpenError {
    fn from_wal(error: WalError) -> Self {
        let text = WalWriteError::Failed(error.to_string());
        match error {
            WalError::SessionMoving { cut_id } => Self::SessionMoving { cut_id },
            WalError::SessionMoved { holder } => Self::SessionMoved { holder },
            WalError::NewerWriter { missing, .. } => WalOpenError::NewerWriter {
                missing,
                error: text,
            },
            _ => WalOpenError::Failed(text),
        }
    }
}

impl std::fmt::Display for WalOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalOpenError::SessionMoving { cut_id } => {
                write!(f, "session_moving {{cutId: {cut_id}}}")
            }
            WalOpenError::SessionMoved { holder } => {
                write!(f, "session_moved {{holder: {holder}}}")
            }
            WalOpenError::NewerWriter { error, .. } | WalOpenError::Failed(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for WalOpenError {}

enum WalCommand {
    #[cfg(test)]
    BudgetSnapshot(std_mpsc::Sender<Option<(u64, u64, u64)>>),
    #[cfg(test)]
    AppendDrained(oneshot::Sender<()>),
    /// Append a record at the caller-assigned `seq`, coalesced (no fsync yet). The
    /// writer VALIDATES `seq` equals its own expected next and poisons on a mismatch
    /// (the handle assigns the seq so a ring entry has its final cursor at publish; the
    /// writer's check is the monotonicity guard).
    Append {
        seq: u64,
        record: Box<WalRecord>,
        frame: Option<Vec<u8>>,
    },
    /// Append `record` to an overhaul log as the version 4 frame `prepared`, which the
    /// enqueueing thread built from that record. The writer thread samples `appended_ms` from
    /// its stamp clock immediately before handing the frame to the log, which stamps the header.
    AppendV4 {
        seq: u64,
        record: Box<WalRecord>,
        prepared: broca_wal::PreparedV4Frame,
    },
    /// fsync everything pending + ack with the last FSYNCED seq (a barrier point).
    Sync(oneshot::Sender<Result<u64, WalWriteError>>),
    /// Final fsync if anything is still dirty, then stop the writer.
    Shutdown(std_mpsc::Sender<Result<(), WalWriteError>>),
}

/// Where a writer thread reads the `appended_ms` it stamps in a version 4 frame. Production
/// uses [`broca_wal::framing::appended_ms_now`], the wall clock held from going backwards within
/// the process; a test can drive its own to see exactly when, and on which thread, it is read.
#[derive(Clone)]
pub(crate) struct StampClock(Arc<dyn Fn() -> u64 + Send + Sync>);

impl StampClock {
    pub(crate) fn wall() -> Self {
        Self(Arc::new(broca_wal::framing::appended_ms_now))
    }

    #[cfg(test)]
    pub(crate) fn from_fn(clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self(Arc::new(clock))
    }

    fn now_ms(&self) -> u64 {
        (self.0)()
    }
}

/// Publishes every record after the writer's fsync makes it durable, in FIFO order.
/// The writer task is the single serialization point; if the bounded receiver falls behind,
/// it receives an explicit `Lagged` result instead of an unmarked sequence gap. The session coordinator sets this to feed the control-stream
/// fan-out: each observed record is projected (`project_control`) into the cursor-tagged
/// control units subscribers see. It's the same projection the attach-replay uses, so a
/// live unit and a replayed unit are identical by construction. `None` (the default) =
/// zero-cost; the per-run CLI/test path never sets it. Publishing only after fsync ensures a
/// live cursor never advances over bytes that a crash could still lose.
///
/// The fan-out can gate this observer off when no subscriber is attached. The writer hot
/// path then pays only a relaxed atomic load instead of cloning large records into the
/// observer channel for nobody.
#[derive(Clone)]
pub struct RecordObserver {
    tx: broadcast::Sender<ObservedRecord>,
    /// The live-or-attaching subscriber COUNT — the writer publishes only while it is
    /// nonzero. A count (not a boolean) so a detach can never race an attach into a
    /// wrongly-disabled gate: the attach's increment is visible on its own, with no
    /// separate flag store a stale detach could overwrite.
    subscribers: Arc<AtomicUsize>,
    tap: Option<(Arc<DurableRecordTap>, Arc<std::sync::atomic::AtomicBool>)>,
}

/// A bounded, non-I/O callback on the dedicated writer thread. Borrowed records are durable;
/// implementations must retain only small metadata, never the conversation or tool payload.
pub type DurableRecordTap =
    dyn Fn(u64, u64, Option<broca_wal::WalLineageId>, &WalRecord) + Send + Sync;

/// One durable record on the writer-to-observer queue, shared behind a pointer.
///
/// Tokio's `broadcast` channel allocates every slot of its bounded buffer when the channel is
/// created, and each slot is as large as the message type. Carrying `FramedRecord` by value made
/// every open session reserve 1,024 record-sized slots (about 600 KB) whether or not anything was
/// ever published, and every new `WalRecord` variant that grew the enum grew all of them. A
/// pointer keeps each slot small and independent of the record's size; the record itself is
/// still cloned once per publish, exactly as before.
#[derive(Clone, Debug)]
pub struct ObservedRecord(Arc<(broca_wal::FramedRecord, Option<broca_wal::WalLineageId>)>);

impl ObservedRecord {
    pub fn new(framed: broca_wal::FramedRecord) -> Self {
        Self(Arc::new((framed, None)))
    }
    pub fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.0 .1
    }
}

impl From<broca_wal::FramedRecord> for ObservedRecord {
    fn from(framed: broca_wal::FramedRecord) -> Self {
        Self::new(framed)
    }
}

impl std::ops::Deref for ObservedRecord {
    type Target = broca_wal::FramedRecord;

    fn deref(&self) -> &broca_wal::FramedRecord {
        &self.0 .0
    }
}

impl RecordObserver {
    pub fn new(tx: broadcast::Sender<ObservedRecord>, subscribers: Arc<AtomicUsize>) -> Self {
        Self {
            tx,
            subscribers,
            tap: None,
        }
    }

    /// Install optional durable metadata observation without cloning records or enabling fan-out.
    /// `healthy` becomes false if the callback panics; its owner must then report unknown state.
    pub fn with_durable_tap(
        mut self,
        tap: Arc<DurableRecordTap>,
        healthy: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.tap = Some((tap, healthy));
        self
    }

    fn observe(
        &self,
        seq: u64,
        fence: u64,
        record: &WalRecord,
        lineage: Option<broca_wal::WalLineageId>,
    ) {
        if self.subscribers.load(Ordering::Acquire) == 0 {
            return;
        }
        // `broadcast` is bounded and non-blocking: when the projector falls behind it drops
        // the oldest queued records and reports `Lagged`, which makes every live subscriber
        // detach and recover from the WAL rather than allowing this writer-side queue to grow.
        let _ = self.tx.send(ObservedRecord(Arc::new((
            broca_wal::FramedRecord {
                seq,
                fence,
                record: record.clone(),
            },
            lineage,
        ))));
    }

    fn observe_tap(
        &self,
        seq: u64,
        fence: u64,
        lineage: Option<broca_wal::WalLineageId>,
        record: &WalRecord,
    ) {
        let Some((tap, healthy)) = &self.tap else {
            return;
        };
        if !healthy.load(Ordering::Acquire) {
            return;
        }
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tap(seq, fence, lineage, record)
        }))
        .is_err()
        {
            healthy.store(false, Ordering::Release);
            tracing::warn!(target: "wal", "durable record tap panicked; disabling its state reporting");
        }
    }
}

#[cfg(test)]
type AfterReserveProbe = Arc<dyn Fn(u64) + Send + Sync>;

#[cfg(test)]
type EnqueueContentionProbe = Arc<dyn Fn() + Send + Sync>;

/// Tags the writer assigned that the run loop has yet to attach to its in-memory prompt.
#[derive(Default)]
struct LiveTags {
    /// By tool call id, oldest first: a model may reuse an id across batches.
    calls: std::collections::HashMap<
        broca_protocol::ToolCallId,
        std::collections::VecDeque<MessageTags>,
    >,
    /// Assistant messages, by the step that produced them.
    steps: std::collections::HashMap<broca_protocol::StepId, MessageTags>,
    /// Budget messages, by their prompt ordinal.
    budgets: std::collections::HashMap<u64, MessageTags>,
}

/// The session lease a writer owned, returned by [`WalHandle::shutdown_into_tenure`] only
/// after the writer thread has exited and its WAL descriptor is closed.
///
/// It carries no append, sync or descriptor capability: it can only keep the session
/// locked and report its epoch, and dropping it releases the lease. It is opaque and
/// non-`Clone`, and nothing converts a fence integer or a raw lease handle into it, so
/// holding one proves a writer for this tenure has already closed.
#[derive(Debug)]
pub struct ClosedWriterTenure {
    lease: Box<dyn LeaseHandle>,
}

impl ClosedWriterTenure {
    /// The epoch stamped on the closed writer's frames. Inspection only.
    pub fn epoch(&self) -> u64 {
        self.lease.epoch()
    }

    /// The namespaced session lease key this tenure still holds.
    pub fn key(&self) -> &cortexkit_lease::LeaseKey {
        self.lease.key()
    }
}

/// A writer's WAL and the session lease beside it, held together until the writer thread
/// takes them. Dropping it closes the WAL before releasing the lease, so no early exit
/// between a writer open and its thread start leaves a writable WAL without its lease.
struct WriterOwner {
    wal: Option<Box<dyn Wal + Send>>,
    lease: Option<Box<dyn LeaseHandle>>,
}

impl WriterOwner {
    fn wal(&self) -> &(dyn Wal + Send) {
        self.wal
            .as_deref()
            .expect("writer owner holds its WAL until handoff")
    }

    fn into_parts(mut self) -> (Box<dyn Wal + Send>, Option<Box<dyn LeaseHandle>>) {
        let wal = self
            .wal
            .take()
            .expect("writer owner holds its WAL until handoff");
        (wal, self.lease.take())
    }
}

impl Drop for WriterOwner {
    fn drop(&mut self) {
        drop(self.wal.take());
        drop(self.lease.take());
    }
}

#[cfg(test)]
thread_local! {
    /// Makes the next writer thread spawn on this thread fail, as an exhausted thread
    /// limit would, so tests can check what an unstarted writer releases and in what order.
    static FAIL_NEXT_WRITER_SPAWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The loop's handle to the writer task.
pub struct WalHandle {
    /// Advisory display placement follows accepted queue order, not the lagging fsync head.
    display_placement: Mutex<Option<crate::placement::Placement>>,
    /// Zero means unlimited; finite limits only decrease over the writer's lifetime.
    input_token_limit: AtomicU64,
    move_check: Option<Arc<dyn Fn() -> Result<(), WalError> + Send + Sync>>,
    media_capture: Option<crate::tool_media::ToolMediaCapture>,
    pending_media: Mutex<std::collections::VecDeque<(u64, crate::compaction::CapturedMedia)>>,
    fence: u64,
    append_budget: Option<Mutex<AppendBudget>>,
    tx: std_mpsc::Sender<WalCommand>,
    /// Reservation order must equal channel-send order because the writer cross-checks
    /// handle-assigned and WAL-assigned seqs and poisons the handle if they drift.
    /// A panic while it is held leaves the reserved-versus-enqueued state unknown, so
    /// `lock_enqueue` fails the writer closed (a sticky writer poison) before recovering the
    /// guard: no later append can reach the WAL, and later calls get errors, not panics.
    enqueue_lock: Arc<Mutex<()>>,
    /// The next seq this handle will assign. HOISTED from the WAL (writer-task-owned)
    /// to the handle (caller-side) so `record` can assign + RETURN a record's final
    /// monotonic seq SYNCHRONOUSLY — a fan-out ring entry then carries its final durable
    /// cursor at publish, with no backpatch. Seeded from the WAL's durable head at spawn.
    next_seq: Arc<AtomicU64>,
    publication_bytes: Arc<AtomicU64>,
    /// Last sequence made durable by a successful writer fsync. Read-only paths clone this
    /// gauge so they can bound visible WAL records without borrowing the writer or taking its
    /// lease.
    durable_head: Arc<AtomicU64>,
    /// Shared poison state: writer-side append/sequence failures and handle-side channel
    /// failures converge here, so no later durability barrier can report success.
    poisoned: Arc<Mutex<Option<String>>>,
    /// The lineage identity of the file the writer task appends to, refreshed by that task
    /// after every append. A legacy file has none until its first append mints one, so a
    /// value read at spawn could be stale; readers go through [`WalHandle::lineage_id`].
    lineage_id: Arc<Mutex<Option<broca_wal::WalLineageId>>>,
    /// Frozen policy proved at reconstruction or by this writer's first run.
    /// Unknown on an inherited log until its complete WAL/checkpoint is read.
    lineage_retention: Mutex<Option<crate::LineageRetention>>,
    /// The lineage's runner-tag state: which tag the next part gets. Known for an empty WAL at
    /// spawn; otherwise seeded by the admission that reconstructed the session (`None` until
    /// then, and nothing is tagged while it is unknown). Read and advanced only under the
    /// enqueue lock, so tags follow WAL order. See [`crate::tags`].
    lineage_tags: Mutex<Option<crate::tags::LineageTags>>,
    /// True when the log reports an overhaul format at spawn: every record then goes to the
    /// writer as a [`broca_wal::PreparedV4Frame`] built on the enqueueing thread, and never as a
    /// legacy frame. Fixed for the writer's lifetime; a log's format does not change under it.
    v4: bool,
    /// Tags assigned to tool results, assistant messages and budget messages, kept until the
    /// run loop attaches them to the messages it adds to its in-memory prompt. (A steer's
    /// messages get their tags from the record the steer inbox writes, see `SteerInbox::deliver`.) Retained only while a run that renders tags
    /// asked for them (`None` otherwise), so a run that does not render tags keeps nothing.
    live_tags: Mutex<Option<LiveTags>>,
    #[cfg(test)]
    after_reserve_probe: Mutex<Option<AfterReserveProbe>>,
    #[cfg(test)]
    enqueue_contention_probe: Mutex<Option<EnqueueContentionProbe>>,
    /// The writer thread hands back the session lease it owned (if any) only after
    /// `writer_loop` has returned and dropped the WAL, so whoever joins it receives a
    /// lease that no longer has a writable descriptor beside it.
    join: Option<std::thread::JoinHandle<Option<Box<dyn LeaseHandle>>>>,
}

impl WalHandle {
    /// Transfer only captures covered by a successful WAL barrier. Appends racing after that
    /// barrier wait for the next one, so an uncommitted steer cannot authorize rendered media.
    pub(crate) fn take_media_captures(&self) -> crate::compaction::CapturedMedia {
        let head = self.durable_head.load(Ordering::SeqCst);
        let mut pending = self.pending_media.lock().unwrap();
        let mut captured = crate::compaction::CapturedMedia::default();
        while pending.front().is_some_and(|(seq, _)| *seq <= head) {
            captured.extend(pending.pop_front().expect("front checked").1);
        }
        captured
    }

    /// Exercise only enqueue's capture work, without counting command boxing or channel storage.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn capture_for_append_for_test(&self, seq: u64, record: &WalRecord) {
        self.capture_for_append(seq, record);
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn has_pending_media_capture_for_test(&self, reference: &broca_protocol::MediaRef) -> bool {
        self.pending_media
            .lock()
            .unwrap()
            .iter()
            .any(|(_, captured)| captured.contains(reference))
    }

    pub fn media_capture(&self) -> Option<crate::tool_media::ToolMediaCapture> {
        self.media_capture.clone()
    }

    pub(crate) fn bind_media_capture(&mut self, store: &impl WalStore, session: &BindIdentity) {
        // Tool media shares attachment admission's debug-only, non-shipping opt-in.
        self.media_capture = if broca_protocol::attachments_enabled() {
            store
                .uploads()
                .map(|uploads| crate::tool_media::ToolMediaCapture::new(uploads, session.clone()))
        } else {
            None
        };
    }

    pub fn check_move(&self) -> Result<(), WalError> {
        match &self.move_check {
            Some(check) => check(),
            None => Ok(()),
        }
    }
    #[cfg(feature = "test-clock")]
    pub fn terminal_reserve_for_test() -> u64 {
        retained_size::reserve_bytes(broca_wal::framing::LINEAGE_FRAME_LEN as u64)
    }

    /// Synchronously prove the already-open WAL, then start its writer task. Async callers that
    /// also need to open the WAL should use [`open_and_spawn_observed`](Self::open_and_spawn_observed)
    /// so recovery I/O does not block an async runtime worker.
    pub fn spawn(wal: Box<dyn Wal + Send>, fence: u64) -> Result<WalHandle, WalWriteError> {
        Self::spawn_observed(wal, fence, None)
    }

    /// Like [`spawn`](Self::spawn) but with an optional [`RecordObserver`] the writer
    /// publishes each appended record to (in durable FIFO order). The coordinator uses
    /// this to drive the control-stream fan-out; the per-run path passes `None`.
    pub fn spawn_observed(
        wal: Box<dyn Wal + Send>,
        fence: u64,
        observer: Option<RecordObserver>,
    ) -> Result<WalHandle, WalWriteError> {
        let (wal, last_seq) = Self::prove_recovery(wal)?;
        Self::spawn_proven_observed(wal, last_seq, fence, observer, None)
    }

    /// Keep the per-run lease on the writer thread, including if its owner is cancelled.
    pub(crate) fn spawn_with_lease(
        wal: Box<dyn Wal + Send>,
        lease: Box<dyn LeaseHandle>,
    ) -> Result<WalHandle, WalWriteError> {
        let fence = lease.epoch();
        let (wal, last_seq) = Self::prove_recovery(wal)?;
        Self::spawn_proven_observed(wal, last_seq, fence, None, Some(lease))
    }

    pub(crate) fn spawn_in_store<W: WalStore>(
        wal: Box<dyn Wal + Send>,
        store: &W,
        session: &BindIdentity,
        fence: u64,
        lease: Option<Box<dyn LeaseHandle>>,
    ) -> Result<WalHandle, WalWriteError> {
        if !wal.wal_config().is_overhaul() {
            return match lease {
                Some(lease) => Self::spawn_with_lease(wal, lease),
                None => Self::spawn(wal, fence),
            };
        }
        let fence = lease.as_ref().map_or(fence, |lease| lease.epoch());
        let (wal, seq) = Self::prove_recovery(wal)?;
        let context = store
            .writer_marker_root()
            .map(|root| open_writers::WriterContext {
                root,
                address: session_addr(session),
            });
        Self::spawn_proven_with_clock(wal, seq, fence, None, lease, (StampClock::wall(), context))
    }

    /// Exercise the production store-aware spawn with a filesystem backend on its writer thread.
    #[cfg(feature = "test-clock")]
    pub fn spawn_in_store_with_fs_for_test<W: WalStore>(
        wal: Box<dyn Wal + Send>,
        store: &W,
        session: &BindIdentity,
        fence: u64,
        lease: Option<Box<dyn LeaseHandle>>,
        fs: Arc<dyn broca_wal::fs_ops::FsOps>,
    ) -> Result<WalHandle, WalWriteError> {
        struct Restore(Option<Arc<dyn broca_wal::fs_ops::FsOps>>);
        impl Drop for Restore {
            fn drop(&mut self) {
                WRITER_FS_FOR_TEST.with(|slot| {
                    slot.replace(self.0.take());
                });
            }
        }
        let _restore = Restore(WRITER_FS_FOR_TEST.with(|slot| slot.replace(Some(fs))));
        Self::spawn_in_store(wal, store, session, fence, lease)
    }

    /// Open and prove a session WAL away from the async runtime, then start its writer thread.
    ///
    /// The lease moves into the blocking operation so cancellation cannot release it while torn-tail
    /// recovery or the recovery fsync is still using the WAL. No handle or durable head exists until
    /// open, recovery sync, and `last_seq` discovery all complete successfully.
    pub async fn open_and_spawn_observed<W>(
        wal_store: Arc<W>,
        session: BindIdentity,
        lease: Box<dyn LeaseHandle>,
        observer: Option<RecordObserver>,
    ) -> Result<(WalHandle, Box<dyn LeaseHandle>), WalOpenError>
    where
        W: WalStore + 'static,
    {
        Self::open_and_spawn_observed_with_retention(wal_store, session, lease, observer, None)
            .await
    }

    /// Like the ordinary open, but with admission's already-validated first-send
    /// retention policy. The lease remains owned through directory selection and recovery.
    pub async fn open_and_spawn_observed_with_retention<W>(
        wal_store: Arc<W>,
        session: BindIdentity,
        lease: Box<dyn LeaseHandle>,
        observer: Option<RecordObserver>,
        retention: Option<u64>,
    ) -> Result<(WalHandle, Box<dyn LeaseHandle>), WalOpenError>
    where
        W: WalStore + 'static,
    {
        Self::open_and_spawn_observed_with_class(
            wal_store, session, lease, observer, retention, None,
        )
        .await
    }

    pub async fn open_and_spawn_observed_with_class<W: WalStore + 'static>(
        wal_store: Arc<W>,
        session: BindIdentity,
        lease: Box<dyn LeaseHandle>,
        observer: Option<RecordObserver>,
        retention: Option<u64>,
        session_class: Option<String>,
    ) -> Result<(WalHandle, Box<dyn LeaseHandle>), WalOpenError> {
        let check_store = wal_store.clone();
        let check_session = session.clone();
        let opened = spawn_blocking(JobKind::WalWriter, move || {
            let fence = lease.epoch();
            let (wal, lease) = wal_store
                .open_writer(&session, retention, session_class.as_deref(), lease)
                .map_err(WalOpenError::from_wal)?;
            let (wal, last_seq) = Self::prove_recovery(wal).map_err(WalOpenError::Failed)?;
            Ok::<_, WalOpenError>((wal, last_seq, fence, lease))
        })
        .await
        .map_err(|error| {
            WalOpenError::Failed(WalWriteError::Failed(format!(
                "WAL open/recovery task failed: {error}"
            )))
        })??;
        let (wal, last_seq, fence, lease) = opened;
        let context = check_store
            .writer_marker_root()
            .map(|root| open_writers::WriterContext {
                root,
                address: session_addr(&check_session),
            });
        let mut handle = Self::spawn_proven_with_clock(
            wal,
            last_seq,
            fence,
            observer,
            None,
            (StampClock::wall(), context),
        )
        .map_err(WalOpenError::Failed)?;
        handle.bind_media_capture(check_store.as_ref(), &check_session);
        handle.move_check = Some(Arc::new(move || {
            check_store.check_move(&check_session, true)
        }));
        Ok((handle, lease))
    }

    /// Open and prove a session WAL under the caller-owned tenure, then start its
    /// writer task which retains that tenure until the WAL is closed.
    ///
    /// The tenure is consumed: the blocking recovery closure owns it until the
    /// descriptor exists, and the writer task owns it after that. No second
    /// acquisition happens inside the store open; a second holder that tries to
    /// acquire the same session while this writer lives gets `Held`. Cancellation
    /// before the await returns leaves authority with the blocking closure until
    /// its descriptor is gone, and cancellation after spawn leaves it with the
    /// draining writer task. The caller gets only the handle, never a
    /// separately droppable lease, so the lock cannot be released early while
    /// the WAL stays writable.
    pub async fn open_and_spawn_owned<W>(
        wal_store: Arc<W>,
        session: BindIdentity,
        tenure: OwnedWalTenure,
        observer: Option<RecordObserver>,
    ) -> Result<WalHandle, WalOpenError>
    where
        W: WalStore + 'static,
    {
        Self::open_and_spawn_owned_with_retention(wal_store, session, tenure, observer, None).await
    }

    /// Like [`Self::open_and_spawn_owned`] but with admission's already-validated
    /// first-send retention policy. The tenure remains owned through directory
    /// selection and recovery.
    pub async fn open_and_spawn_owned_with_retention<W>(
        wal_store: Arc<W>,
        session: BindIdentity,
        tenure: OwnedWalTenure,
        observer: Option<RecordObserver>,
        retention: Option<u64>,
    ) -> Result<WalHandle, WalOpenError>
    where
        W: WalStore + 'static,
    {
        Self::open_and_spawn_owned_with_class(wal_store, session, tenure, observer, retention, None)
            .await
    }

    pub async fn open_and_spawn_owned_with_class<W: WalStore + 'static>(
        wal_store: Arc<W>,
        session: BindIdentity,
        tenure: OwnedWalTenure,
        observer: Option<RecordObserver>,
        retention: Option<u64>,
        session_class: Option<String>,
    ) -> Result<WalHandle, WalOpenError> {
        // Bind before touching I/O: a wrong tenure refuses without creating
        // directories or files, truncating a torn tail, or writing lineage.
        // The tenure is consumed on both paths.
        if let Err(error) = tenure.require_session(&session) {
            return Err(WalOpenError::Failed(WalWriteError::Failed(format!(
                "owned WAL open refused: {error}"
            ))));
        }
        // The completed open, with deliberate teardown order. On cancellation
        // the blocking task still runs to completion and its result is dropped
        // without a receiver: this Drop closes the writable descriptor before
        // the tenure releases, even on that path. Success moves both out and
        // leaves this guard empty, so its drop is a no-op.
        struct OpenedOwned {
            wal: Option<Box<dyn Wal + Send>>,
            lease: Option<Box<dyn LeaseHandle>>,
            last_seq: u64,
            fence: u64,
        }
        impl Drop for OpenedOwned {
            fn drop(&mut self) {
                let wal = self.wal.take();
                drop(wal);
                let lease = self.lease.take();
                drop(lease);
            }
        }
        let check_store = wal_store.clone();
        let check_session = session.clone();
        let opened = spawn_blocking(JobKind::WalWriter, move || {
            // Authority stays with this closure until the descriptor is gone:
            // no second acquisition happens here, the caller-owned lease is
            // reused through the store open, which also uses it to authorize
            // restoring an archived log before the descriptor exists.
            let lease = tenure.into_lease();
            let fence = lease.epoch();
            let (wal, lease) = wal_store
                .open_owned_writer(&session, retention, session_class.as_deref(), lease)
                .map_err(WalOpenError::from_wal)?;
            // `prove_recovery` drops the descriptor before returning its
            // error, so by the time its Err reaches here the WAL is already
            // closed and only the tenure remains in the guard below.
            let (wal, last_seq) = Self::prove_recovery(wal).map_err(WalOpenError::Failed)?;
            Ok::<_, WalOpenError>(OpenedOwned {
                wal: Some(wal),
                lease: Some(lease),
                last_seq,
                fence,
            })
        })
        .await
        .map_err(|error| {
            WalOpenError::Failed(WalWriteError::Failed(format!(
                "WAL open/recovery task failed: {error}"
            )))
        })??;
        let mut opened = opened;
        let wal = opened
            .wal
            .take()
            .expect("owned open holds its WAL until handoff");
        let lease = opened
            .lease
            .take()
            .expect("owned open holds its tenure until handoff");
        let (last_seq, fence) = (opened.last_seq, opened.fence);
        // The writer task owns the lease until the WAL is closed: dropping the
        // handle without shutdown still joins through the task, and shutdown
        // joins before the task drops the lease. No separate lease is returned.
        let context = check_store
            .writer_marker_root()
            .map(|root| open_writers::WriterContext {
                root,
                address: session_addr(&check_session),
            });
        let mut handle = Self::spawn_proven_with_clock(
            wal,
            last_seq,
            fence,
            observer,
            Some(lease),
            (StampClock::wall(), context),
        )
        .map_err(WalOpenError::Failed)?;
        handle.bind_media_capture(check_store.as_ref(), &check_session);
        handle.move_check = Some(Arc::new(move || {
            check_store.check_move(&check_session, true)
        }));
        Ok(handle)
    }

    fn prove_recovery(
        wal: Box<dyn Wal + Send>,
    ) -> Result<(Box<dyn Wal + Send>, u64), WalWriteError> {
        // FileWal::open has already recovered any torn tail under the caller's lease. Prove the
        // remaining complete prefix before publishing its sequence as durable: flushed bytes can
        // be readable even when a prior fsync failed. This is the one recovery barrier per spawn.
        wal.sync()
            .map_err(|error| WalWriteError::Failed(error.to_string()))?;
        let last_seq = wal.last_seq();
        Ok((wal, last_seq))
    }

    fn spawn_proven_observed(
        wal: Box<dyn Wal + Send>,
        last_seq: u64,
        fence: u64,
        observer: Option<RecordObserver>,
        lease: Option<Box<dyn LeaseHandle>>,
    ) -> Result<WalHandle, WalWriteError> {
        Self::spawn_proven_with_clock(
            wal,
            last_seq,
            fence,
            observer,
            lease,
            (StampClock::wall(), None),
        )
    }

    fn spawn_proven_with_clock(
        wal: Box<dyn Wal + Send>,
        last_seq: u64,
        fence: u64,
        observer: Option<RecordObserver>,
        lease: Option<Box<dyn LeaseHandle>>,
        startup: (StampClock, Option<open_writers::WriterContext>),
    ) -> Result<WalHandle, WalWriteError> {
        let (clock, context) = startup;
        // One owner holds the WAL and its lease from here until the writer thread takes
        // them, so a panic while preparing the handle or a failed thread spawn still
        // closes the WAL before the lease is released.
        let owner = WriterOwner {
            wal: Some(wal),
            lease,
        };
        let wal = owner.wal();
        let v4 = wal.wal_config().is_overhaul();
        if v4 && context.is_none() {
            return Err(WalWriteError::Failed(
                "writer-id/v1: overhaul spawn requires a store marker root and session address"
                    .into(),
            ));
        }
        let publisher = if v4 {
            let path = wal.log_path().ok_or(WalWriteError::ProofPathUnavailable)?;
            let lineage = wal.lineage_id().ok_or_else(|| {
                WalWriteError::Failed(
                    "visibility-late/v1: overhaul WAL has no lineage for its durable-head proof"
                        .into(),
                )
            })?;
            Some(broca_wal::head_proof::Publisher::new(
                path.to_path_buf(),
                lineage,
            ))
        } else {
            None
        };
        // Lifecycle records do not start a conversation. A warmed writer-only log still needs
        // the same first-send placement, retention and runner-tag state as an empty log.
        let unstarted = last_seq == 0
            || (v4
                && wal
                    .replay()
                    .map_err(|error| WalWriteError::Failed(error.to_string()))?
                    .iter()
                    .all(|frame| {
                        matches!(
                            frame.record,
                            WalRecord::WriterOpened { .. }
                                | WalRecord::WriterClosed { .. }
                                | WalRecord::VisibilityLate { .. }
                        )
                    }));
        let last_seq = last_seq + u64::from(v4);
        // Seq is 1-based; the first record of an empty WAL is seq 1, so the next seq
        // after a durable head of `last_seq` is `last_seq + 1`.
        let next_seq = Arc::new(AtomicU64::new(last_seq + 1));
        let enqueue_lock = Arc::new(Mutex::new(()));
        let publication_bytes = Arc::new(AtomicU64::new(0));
        let writer_sequence = (
            next_seq.clone(),
            enqueue_lock.clone(),
            publication_bytes.clone(),
        );
        let durable_head = Arc::new(AtomicU64::new(last_seq));
        let writer_durable_head = Arc::clone(&durable_head);
        let poisoned = Arc::new(Mutex::new(None));
        let writer_poisoned = poisoned.clone();
        let lineage_id = Arc::new(Mutex::new(wal.lineage_id()));
        let writer_lineage_id = Arc::clone(&lineage_id);
        let budget_scope = wal.append_budget_scope();
        let recovery_cap = wal.recovery_byte_cap();
        let make_budget = |budget: Option<(u64, u64, u64)>, writer_close_reserve| {
            budget.map(|(max_bytes, used_bytes, overhead)| {
                Mutex::new(AppendBudget {
                    writer_close_reserve,
                    retained: budget_scope == retained_size::SizeScope::RetainedSession,
                    session_max_bytes: recovery_cap.unwrap_or(max_bytes),
                    max_bytes,
                    used_bytes,
                    overhead,
                    reserve: retained_size::reserve_bytes(overhead),
                    refused: false,
                })
            })
        };
        let legacy_budget = (!v4).then(|| make_budget(wal.append_budget(), 0));
        // The log reports the format its store was configured with at boot. An overhaul log
        // stamps its own `writer_id` per open and raises the feature floor before each frame.
        let (tx, rx) = std_mpsc::channel::<WalCommand>();
        let (ready_tx, ready_rx) = std_mpsc::sync_channel(1);
        let body = move || {
            // writer_loop drops its WAL before the lease is released, even on unwind:
            // the lease is bound outside writer_loop's frame, so an unwinding writer
            // drops the WAL first. On a normal return the lease goes back to the
            // joiner, which releases it or hands it over (see `shutdown_into_tenure`).
            let mut owner = owner;
            let wal = owner
                .wal
                .as_mut()
                .expect("writer owner holds its WAL during startup");
            let mut publication = Publication {
                publisher,
                sequence: Some(writer_sequence),
                deferred: Default::default(),
                fence,
                clock: clock.clone(),
                observed: observer.is_some(),
            };
            let writer = if let Some(context) = context {
                let address = context.address.clone();
                match context.open(&mut **wal, fence, || clock.now_ms()) {
                    Ok((writer, seq, record)) => {
                        let mut pending = vec![(seq, Box::new(record))];
                        let opened = publication.sync(&mut **wal, &rx, &mut pending);
                        if let Err(error) = opened {
                            let _ = ready_tx.send(Err(WalWriteError::Failed(error)));
                            let (wal, lease) = owner.into_parts();
                            drop(wal);
                            return lease;
                        }
                        writer_durable_head.store(wal.last_seq(), Ordering::Release);
                        if let Some(obs) = &observer {
                            for (seq, record) in pending {
                                obs.observe(seq, fence, &record, wal.lineage_id());
                                obs.observe_tap(seq, fence, wal.lineage_id(), &record);
                            }
                        }
                        Some(writer)
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(WalWriteError::Failed(format!(
                            "writer-id/v1: cannot open writer for {address}: {error}"
                        ))));
                        let (wal, lease) = owner.into_parts();
                        drop(wal);
                        return lease;
                    }
                }
            } else {
                None
            };
            if v4 {
                let _ = ready_tx.send(Ok((wal.append_budget(), wal.writer_close_reserve())));
            }
            let (wal, lease) = owner.into_parts();
            writer_loop_with_identity(
                wal,
                fence,
                rx,
                observer,
                writer_poisoned,
                writer_durable_head,
                writer_lineage_id,
                clock,
                writer,
                publication,
            );
            lease
        };
        #[cfg(feature = "test-clock")]
        let body = {
            let fs = WRITER_FS_FOR_TEST.with(|slot| slot.borrow().clone());
            move || match fs {
                Some(fs) => broca_wal::fs_ops::with_fs_ops(fs, body),
                None => body(),
            }
        };
        #[cfg(test)]
        if FAIL_NEXT_WRITER_SPAWN.with(|fail| fail.replace(false)) {
            // Mirrors a real spawn failure, which drops the unstarted closure.
            drop(body);
            return Err(WalWriteError::Failed(
                "WAL writer thread spawn failed: injected".into(),
            ));
        }
        // recv() waits for the lifetime of the session, including while idle. A
        // resident writer must not consume a slot needed by finite blocking jobs.
        let join = std::thread::Builder::new()
            .name("broca-wal-writer".into())
            .spawn(body)
            .map_err(|error| {
                WalWriteError::Failed(format!("WAL writer thread spawn failed: {error}"))
            })?;
        let append_budget = if let Some(budget) = legacy_budget {
            budget
        } else {
            match ready_rx.recv() {
                Ok(Ok((budget, close_reserve))) => make_budget(budget, close_reserve),
                error => {
                    let _ = join.join();
                    return Err(match error {
                        Ok(Err(error)) => error,
                        _ => WalWriteError::Failed(
                            "writer-id/v1: writer startup did not complete".into(),
                        ),
                    });
                }
            }
        };
        // Startup markers are already included in the budget returned by the WAL.
        // Only later publication markers need this separate enqueue-side charge.
        publication_bytes.store(0, Ordering::SeqCst);
        Ok(WalHandle {
            display_placement: Mutex::new(unstarted.then(Default::default)),
            input_token_limit: AtomicU64::new(0),
            move_check: None,
            media_capture: None,
            pending_media: Mutex::default(),
            fence,
            append_budget,
            v4,
            tx,
            enqueue_lock,
            next_seq,
            publication_bytes,
            durable_head,
            poisoned,
            lineage_id,
            lineage_retention: Mutex::new(unstarted.then_some(crate::LineageRetention::Unstarted)),
            lineage_tags: Mutex::new(unstarted.then(crate::tags::LineageTags::unstarted)),
            live_tags: Mutex::new(None),
            #[cfg(test)]
            after_reserve_probe: Mutex::new(None),
            #[cfg(test)]
            enqueue_contention_probe: Mutex::new(None),
            join: Some(join),
        })
    }

    /// The next seq this handle will reserve. This is a consistency hint, not a
    /// durability barrier: callers use it only to decide whether an already-decoded
    /// WAL snapshot still matches the writer head before the loop appends anything.
    pub fn next_record_seq(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst)
    }

    pub fn needs_display_placement_seed(&self) -> bool {
        self.display_placement.lock().unwrap().is_none()
    }

    /// A reconstructed prefix may seed only the exact queue head it describes. Never replace
    /// a display placement this writer has already advanced, even if reconstruction raced an enqueue.
    pub fn seed_display_placement(&self, head: u64, placement: crate::placement::Placement) {
        let _guard = self.lock_enqueue();
        let mut current = self.display_placement.lock().unwrap();
        if current.is_none() && self.next_record_seq() == head.saturating_add(1) {
            *current = Some(placement);
        }
    }

    /// Read immediately after queuing StepStarted and carry the value with its output.
    pub fn display_ordinal(&self) -> Option<u64> {
        self.display_placement
            .lock()
            .unwrap()
            .as_ref()
            .map(|p| p.next_ordinal())
    }

    /// Clone the lock-free gauge for the last successfully fsynced WAL sequence.
    pub fn durable_head(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.durable_head)
    }

    /// The lineage identity of the log this writer appends to, as of the writer's last
    /// processed append. `None` for a legacy file whose first append has not been processed
    /// yet (a [`sync`](Self::sync) barrier settles that), or for a log with no identity.
    pub fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        *self.lineage_id.lock().unwrap()
    }

    pub(crate) fn seed_lineage_retention(&self, policy: Option<crate::LineageRetention>) {
        let mut current = self.lineage_retention.lock().unwrap();
        if current.is_none() {
            *current = policy;
        }
    }

    /// Seed the lineage's tag state from a reconstruction of its WAL. Replaces an unknown or
    /// unstarted state (an unstarted one may gain a deleted predecessor's floor); a started
    /// state this writer has already advanced is kept.
    pub fn seed_lineage_tags(&self, tags: Option<crate::tags::LineageTags>) {
        let _guard = self.lock_enqueue();
        let mut current = self.lineage_tags.lock().unwrap();
        if let (None | Some(crate::tags::LineageTags::Unstarted { .. }), Some(tags)) =
            (*current, tags)
        {
            *current = Some(tags);
        }
    }

    /// The lineage's tag state as this writer holds it.
    pub fn lineage_tags(&self) -> Option<crate::tags::LineageTags> {
        *self.lineage_tags.lock().unwrap()
    }

    /// Keep (or stop keeping) the tags the run loop attaches to the messages it adds.
    pub fn retain_live_tags(&self, retain: bool) {
        *self.live_tags.lock().unwrap() = retain.then(LiveTags::default);
    }

    /// The tag assigned to the oldest kept result of call `id`.
    pub fn take_call_tags(&self, id: &broca_protocol::ToolCallId) -> Option<MessageTags> {
        let mut guard = self.live_tags.lock().unwrap();
        let live = guard.as_mut()?;
        let queue = live.calls.get_mut(id)?;
        let tags = queue.pop_front();
        if queue.is_empty() {
            live.calls.remove(id);
        }
        tags
    }

    /// The tags assigned to the assistant message of step `step_id`.
    pub fn take_step_tags(&self, step_id: broca_protocol::StepId) -> Option<MessageTags> {
        self.live_tags
            .lock()
            .unwrap()
            .as_mut()?
            .steps
            .remove(&step_id)
    }

    /// The tags assigned to the budget message at prompt ordinal `ordinal`.
    pub fn take_budget_tags(&self, ordinal: u64) -> Option<MessageTags> {
        self.live_tags
            .lock()
            .unwrap()
            .as_mut()?
            .budgets
            .remove(&ordinal)
    }

    /// Queue `record` like [`record`](Self::record) and also return the runner tags the writer
    /// assigned to it, one entry per message it carries.
    pub fn record_tagged(&self, record: WalRecord) -> (RecordOutcome, Option<Vec<MessageTags>>) {
        let _guard = self.lock_enqueue();
        let mut assigned = None;
        let outcome = self.enqueue_record_with(record, false, &mut assigned);
        (outcome, assigned)
    }

    /// The first run's policy, if this handle has proved it. Unknown is not forever.
    pub fn is_retention_lineage(&self) -> Option<bool> {
        match *self.lineage_retention.lock().unwrap() {
            Some(crate::LineageRetention::Retained { .. }) => Some(true),
            Some(crate::LineageRetention::Forever) => Some(false),
            _ => None,
        }
    }

    /// True once an append or durability barrier has failed. A poisoned writer can never safely
    /// acknowledge another durability-bearing command and must be torn down by its owner.
    pub fn is_poisoned(&self) -> bool {
        current_poison(&self.poisoned).is_some()
    }

    /// Return the sticky poison message for actor teardown and fail-closed command replies.
    pub fn poison_error(&self) -> Option<String> {
        current_poison(&self.poisoned)
    }

    /// Queue a record and return explicit acceptance or refusal. Accepted sequences are
    /// not durable until sync succeeds. Callers that discard the result must sync before
    /// making a durability claim; refused appends have no sequence to publish.
    pub fn record(&self, record: WalRecord) -> RecordOutcome {
        let _guard = self.lock_enqueue();
        self.enqueue_record(record, false)
    }

    /// Assign the record's runner tags, under the enqueue lock, and keep the ones the run loop
    /// asked for. A record whose parts need tags is refused while the lineage's tag state is
    /// unknown (a writer reopened over a non-empty WAL that no admission has seeded): writing
    /// it untagged would leave an untagged part in a tagged lineage, and guessing a number
    /// could reuse one.
    fn stamp_tags(&self, record: &mut WalRecord) -> Result<Option<Vec<MessageTags>>, String> {
        let mut state = self.lineage_tags.lock().unwrap();
        let assigned = match state.as_mut() {
            Some(state) => crate::tags::stamp(state, record),
            None if crate::tags::tag_shape(record).is_some() && record.runner_tags().is_empty() => {
                return Err(
                    "a record with message parts was appended before the lineage's runner-tag                      state was seeded; refusing to write it untagged"
                        .into(),
                );
            }
            None => None,
        };
        drop(state);
        if let Some(assigned) = &assigned {
            if let Some(live) = self.live_tags.lock().unwrap().as_mut() {
                match record {
                    WalRecord::ToolResult { result, .. } => live
                        .calls
                        .entry(result.tool_call_id.clone())
                        .or_default()
                        .push_back(assigned[0].clone()),
                    WalRecord::ToolNoDispatch { tool_call_id, .. }
                    | WalRecord::ApprovalRecorded {
                        call_id: tool_call_id,
                        ..
                    } => live
                        .calls
                        .entry(tool_call_id.clone())
                        .or_default()
                        .push_back(assigned[0].clone()),
                    WalRecord::ModelStepFinished { step_id, .. } => {
                        live.steps.insert(*step_id, assigned[0].clone());
                    }
                    WalRecord::BudgetFired { ordinal, .. } => {
                        live.budgets.insert(*ordinal, assigned[0].clone());
                    }
                    // An episode boundary: nothing kept from the episode before is still owed.
                    WalRecord::RunStarted { .. } => *live = LiveTags::default(),
                    _ => {}
                }
            }
        }
        Ok(assigned)
    }

    pub fn seed_input_token_limit(&self, limit: Option<u64>) {
        if let Some(limit) = limit {
            let mut old = self.input_token_limit.load(Ordering::SeqCst);
            loop {
                let new = if old == 0 { limit } else { old.min(limit) };
                match self.input_token_limit.compare_exchange_weak(
                    old,
                    new,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(current) => old = current,
                }
            }
        }
    }

    pub fn input_token_limit(&self) -> Option<u64> {
        match self.input_token_limit.load(Ordering::SeqCst) {
            0 => None,
            limit => Some(limit),
        }
    }

    fn enqueue_record(&self, record: WalRecord, reserved_terminal: bool) -> RecordOutcome {
        self.enqueue_record_with(record, reserved_terminal, &mut None)
    }

    fn enqueue_record_with(
        &self,
        mut record: WalRecord,
        reserved_terminal: bool,
        assigned: &mut Option<Vec<MessageTags>>,
    ) -> RecordOutcome {
        // Observe time once at enqueue, before encoding or publication. Recovered records
        // retain an existing observation rather than acquiring a restart-time replacement.
        match &mut record {
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: at,
                ..
            }
            | WalRecord::ToolNoDispatch {
                dispatched_at_ms: at,
                ..
            }
            | WalRecord::ToolResult {
                finished_at_ms: at, ..
            }
            | WalRecord::ToolCallDeferred {
                deferred_at_ms: at, ..
            } if at.is_none() => {
                *at = crate::run::epoch_millis();
            }
            WalRecord::ApprovalRecorded {
                closed_at_ms,
                closing_result: Some(_),
                ..
            } if closed_at_ms.is_none() => {
                *closed_at_ms = crate::run::epoch_millis();
            }
            _ => {}
        }
        *assigned = match self.stamp_tags(&mut record) {
            Ok(assigned) => assigned,
            Err(error) => {
                // Fail-stop like any other append failure: the caller's next barrier fails, so
                // no durability claim can cover the missing record.
                set_poison(&self.poisoned, error.clone());
                return RecordOutcome::Refused(WalWriteError::Failed(error));
            }
        };
        // On an overhaul log, the record's version 4 frame, serialized and digested here so the
        // writer thread only stamps its header. Prepared before the split below because both arms
        // send it. Fail-stop like any other append failure: preparing poisons the handle itself.
        let prepared = match self.prepare_v4(&record) {
            Ok(prepared) => prepared,
            Err(error) => return RecordOutcome::Refused(error),
        };
        let (seq, frame) = if let Some(budget) = &self.append_budget {
            let seq = self.next_seq.load(Ordering::SeqCst);
            let mut budget = budget.lock().unwrap();
            if matches!(
                &record,
                WalRecord::RunStarted {
                    retention: None,
                    ..
                }
            ) && *self.lineage_retention.lock().unwrap()
                == Some(crate::LineageRetention::Unstarted)
            {
                if budget.retained {
                    budget.max_bytes = budget.session_max_bytes;
                }
                budget.retained = false;
                budget.refused = false;
            }
            {
                if budget.refused && !reserved_terminal {
                    return RecordOutcome::Refused(budget.refusal());
                }
                // Framing runs broca-wal's per-record feature logic while this holds both the
                // enqueue lock and the budget lock. A panic there must not unwind through
                // them: it would poison both, and every later call on this handle would panic
                // on the poisoned locks. It fails this append like an encode error instead.
                //
                // An overhaul log measures its budget by the version 4 frame prepared above, which
                // is exactly the size the log will write; only a legacy log encodes here.
                let (frame_len, frame) = match &prepared {
                    Some(prepared) => (prepared.frame_len(), None),
                    None => {
                        let encoded =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                broca_wal::framing::encode(seq, self.fence, &record)
                            }));
                        let frame = match encoded {
                            Ok(Ok(frame)) => frame,
                            Ok(Err(error)) => {
                                set_poison(&self.poisoned, error.to_string());
                                return RecordOutcome::Refused(WalWriteError::Failed(
                                    error.to_string(),
                                ));
                            }
                            Err(panic) => {
                                let error = format!(
                                    "framing WAL record {seq} panicked: {}",
                                    panic_text(panic.as_ref())
                                );
                                set_poison(&self.poisoned, error.clone());
                                return RecordOutcome::Refused(WalWriteError::Failed(error));
                            }
                        };
                        (frame.len(), Some(frame))
                    }
                };
                let end = budget
                    .used_bytes
                    .saturating_add(frame_len as u64)
                    .saturating_add(budget.overhead);
                let reserve =
                    if reserved_terminal || matches!(record, WalRecord::RunFinished { .. }) {
                        0
                    } else {
                        budget.reserve
                    };
                if end
                    .saturating_add(self.publication_bytes.load(Ordering::SeqCst))
                    .saturating_add(reserve)
                    .saturating_add(budget.writer_close_reserve)
                    > budget.max_bytes
                {
                    budget.refused = true;
                    return RecordOutcome::Refused(budget.refusal());
                }
                budget.used_bytes = end;
                if reserved_terminal {
                    budget.refused = false;
                }
                self.next_seq.fetch_add(1, Ordering::SeqCst);
                (seq, frame)
            }
        } else {
            (self.next_seq.fetch_add(1, Ordering::SeqCst), None)
        };
        if let WalRecord::RunStarted { retention, .. } = &record {
            let mut policy = self.lineage_retention.lock().unwrap();
            if *policy == Some(crate::LineageRetention::Unstarted) {
                *policy = Some(match retention {
                    Some(seconds) => crate::LineageRetention::Retained { seconds: *seconds },
                    None => crate::LineageRetention::Forever,
                });
            }
        }
        #[cfg(test)]
        self.run_after_reserve_probe(seq);
        {
            let mut placement = self.display_placement.lock().unwrap();
            if placement
                .as_mut()
                .is_some_and(|p| p.observe(&record).is_err())
            {
                // Display attribution cannot make an otherwise valid durable append fail.
                *placement = None;
            }
        }
        match &record {
            WalRecord::InputTokenCapFrozen { limit } => self.seed_input_token_limit(Some(*limit)),
            WalRecord::Queued { request, .. } => self.seed_input_token_limit(
                request
                    .budget
                    .as_ref()
                    .and_then(|budget| budget.max_input_tokens),
            ),
            _ => {}
        }
        self.capture_for_append(seq, &record);
        let command = match prepared {
            Some(prepared) => WalCommand::AppendV4 {
                seq,
                record: Box::new(record),
                prepared,
            },
            None => WalCommand::Append {
                seq,
                record: Box::new(record),
                frame,
            },
        };
        if self.tx.send(command).is_err() {
            let error = format!("append command for seq {seq} could not reach the writer task");
            set_poison(&self.poisoned, error.clone());
            return RecordOutcome::Refused(WalWriteError::Failed(error));
        }
        RecordOutcome::Accepted(seq)
    }

    /// Serialize and digest a record's version 4 frame on this thread, so the writer thread only
    /// stamps its header. `None` on a legacy log, which keeps today's frames. An encode error or a
    /// panic in the codec poisons the handle and is returned; nothing falls back to another
    /// encoding.
    fn prepare_v4(
        &self,
        record: &WalRecord,
    ) -> Result<Option<broca_wal::PreparedV4Frame>, WalWriteError> {
        if !self.v4 {
            return Ok(None);
        }
        let prepared = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            broca_wal::PreparedV4Frame::prepare(record)
        }));
        let error = match prepared {
            Ok(Ok(prepared)) => return Ok(Some(prepared)),
            Ok(Err(error)) => error.to_string(),
            Err(panic) => format!(
                "framing a version 4 WAL record panicked: {}",
                panic_text(panic.as_ref())
            ),
        };
        set_poison(&self.poisoned, error.clone());
        Err(WalWriteError::Failed(error))
    }

    fn capture_for_append(&self, seq: u64, record: &WalRecord) {
        if crate::compaction::CapturedMedia::captures_any(record) {
            let mut captures = crate::compaction::CapturedMedia::default();
            captures.fold(record);
            if !captures.is_empty() {
                self.pending_media
                    .lock()
                    .unwrap()
                    .push_back((seq, captures));
            }
        }
    }

    fn size_scope(&self) -> retained_size::SizeScope {
        self.append_budget
            .as_ref()
            .map_or(retained_size::SizeScope::Session, |budget| {
                budget.lock().unwrap().scope()
            })
    }

    pub(crate) fn clear_size_refusal(&self) {
        let _guard = self.lock_enqueue();
        if let Some(budget) = &self.append_budget {
            budget.lock().unwrap().refused = false;
        }
    }

    pub(crate) fn record_size_refusal_terminal(
        &self,
        max_bytes: u64,
        ts_ms: Option<u64>,
        usage: broca_protocol::Usage,
        retries_used: u32,
    ) -> Result<u64, WalWriteError> {
        self.record_terminal(retained_size::scoped_terminal(
            max_bytes,
            self.size_scope(),
            ts_ms,
            usage,
            retries_used,
        ))
    }

    /// Recovery may run out of room for resume and batch close-out records. Its terminal
    /// bypasses an earlier budget refusal. Oversized diagnostics are replaced by the bounded
    /// size-refusal error; the terminal reason and accounting are preserved.
    pub fn record_terminal(&self, mut record: WalRecord) -> Result<u64, WalWriteError> {
        let _guard = self.lock_enqueue();
        if !matches!(record, WalRecord::RunFinished { .. }) {
            return Err(WalWriteError::Failed(
                "reserved append is not a terminal".into(),
            ));
        }
        let first = self.enqueue_record(record.clone(), true).into_result();
        if matches!(
            first,
            Err(WalWriteError::RetainedSessionTooLarge { .. }
                | WalWriteError::SessionTooLarge { .. })
        ) {
            if let WalRecord::RunFinished { error, .. } = &mut record {
                let max = self
                    .append_budget
                    .as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .max_bytes;
                *error = Some(retained_size::scoped_error(max, self.size_scope()));
            }
            return self.enqueue_record(record, true).into_result();
        }
        first
    }

    /// Sync accepted recovery records even if an optional append was refused. Real I/O
    /// failures still abort sealing; only a byte-budget refusal can use the reserved terminal.
    pub async fn sync_for_seal(&self) -> Result<u64, WalWriteError> {
        match self.sync().await {
            Err(
                WalWriteError::RetainedSessionTooLarge { .. }
                | WalWriteError::SessionTooLarge { .. },
            ) => Ok(self.durable_head.load(Ordering::Acquire)),
            result => result,
        }
    }

    /// Queue the assistant step and rejected-call results without allowing another
    /// producer to fsync between them. These are still separate WAL frames: recovery
    /// must handle a crash that preserves the step but loses the later error results.
    pub(crate) fn record_group(&self, records: Vec<WalRecord>) -> Result<(), WalWriteError> {
        let _guard = self.lock_enqueue();
        for record in records {
            self.enqueue_record(record, false).into_result()?;
        }
        Ok(())
    }

    /// Barrier: fsync everything appended so far, await durability. Returns the last
    /// FSYNCED WAL seq — the durable head a control-stream cursor may advance to (NOT
    /// `last_seq`, which counts coalesced appends past the fsync barrier). Returns the
    /// FATAL write error if any append/fsync failed.
    pub async fn sync(&self) -> Result<u64, WalWriteError> {
        // Queue the barrier after every already-reserved append. This preserves the claim
        // that a successful ack covers everything callers had reserved before sync began.
        let (ack_rx, send_failed) = {
            let _guard = self.lock_enqueue();
            if let Some(error) = current_poison(&self.poisoned) {
                return Err(WalWriteError::Failed(error));
            }
            let (ack_tx, ack_rx) = oneshot::channel();
            let send_failed = self.tx.send(WalCommand::Sync(ack_tx)).is_err();
            (ack_rx, send_failed)
        };
        // The enqueue guard must be released before fsync completes so record production
        // remains independent of the blocking durability operation.
        if send_failed {
            let error = "writer task is gone".to_string();
            set_poison(&self.poisoned, error.clone());
            return Err(WalWriteError::Failed(error));
        }
        match ack_rx.await {
            Ok(Ok(head)) => {
                if let Some(budget) = &self.append_budget {
                    let budget = budget.lock().unwrap();
                    if budget.refused {
                        return Err(budget.refusal());
                    }
                }
                Ok(head)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => {
                let error = "writer task dropped the ack".to_string();
                set_poison(&self.poisoned, error.clone());
                Err(WalWriteError::Failed(error))
            }
        }
    }

    /// Drain + final fsync IF DIRTY + join the writer BEFORE the lease releases (so a
    /// superseded writer can never append after a new run acquires the session).
    pub async fn shutdown(mut self) -> Result<(), WalWriteError> {
        // The blocking job owns the whole handle, so cancelling this await cannot detach
        // a join from the writer's lifetime. Drop also joins if this future is never polled.
        tokio::task::spawn_blocking(move || self.shutdown_blocking())
            .await
            .map_err(|error| WalWriteError::Failed(format!("WAL shutdown task failed: {error}")))?
    }

    fn shutdown_blocking(&mut self) -> Result<(), WalWriteError> {
        let (result, lease) = self.shutdown_blocking_keeping_lease();
        // The writer thread has exited and dropped its WAL; only now may the lease go.
        drop(lease);
        result
    }

    /// Close the writer and keep the session lease it owned.
    ///
    /// The writer drains, performs its final sync if dirty, and exits; its thread is
    /// joined, so the descriptor is gone before this returns. The still-held lease comes
    /// back as a [`ClosedWriterTenure`], which has no append or descriptor capability and
    /// releases the lease when dropped. A failed final sync still returns the tenure,
    /// because the descriptor is closed either way and releasing early would let another
    /// owner open the log while the failure is being handled. A writer that never owned a
    /// lease (the guarded standalone spawn) returns no tenure.
    ///
    /// A move of an idle session uses this to take over the session lease from its actor
    /// without the lock ever being free between the writer closing and the move
    /// publishing its markers.
    pub async fn shutdown_into_tenure(
        mut self,
    ) -> (Result<(), WalWriteError>, Option<ClosedWriterTenure>) {
        match tokio::task::spawn_blocking(move || self.shutdown_blocking_keeping_lease()).await {
            Ok((result, lease)) => (result, lease.map(|lease| ClosedWriterTenure { lease })),
            Err(error) => (
                Err(WalWriteError::Failed(format!(
                    "WAL shutdown task failed: {error}"
                ))),
                None,
            ),
        }
    }

    fn shutdown_blocking_keeping_lease(
        &mut self,
    ) -> (Result<(), WalWriteError>, Option<Box<dyn LeaseHandle>>) {
        let Some(join) = self.join.take() else {
            return (
                current_poison(&self.poisoned)
                    .map_or(Ok(()), |error| Err(WalWriteError::Failed(error))),
                None,
            );
        };
        let (ack, result) = std_mpsc::channel();
        let sent = self.tx.send(WalCommand::Shutdown(ack));
        // Always join, even when the channel or final sync failed. No writer may
        // survive its owning handle and continue appending under a released lease.
        let lease = match join.join() {
            Ok(lease) => lease,
            Err(_) => {
                // Unwinding already dropped the WAL and then the lease.
                set_poison(
                    &self.poisoned,
                    "WAL writer thread panicked during shutdown".into(),
                );
                None
            }
        };
        if sent.is_err() {
            set_poison(&self.poisoned, "WAL writer closed before shutdown".into());
        }
        if let Some(error) = current_poison(&self.poisoned) {
            return (Err(WalWriteError::Failed(error)), lease);
        }
        let result = result.recv().unwrap_or_else(|_| {
            let error = "WAL writer dropped the shutdown acknowledgement".to_string();
            set_poison(&self.poisoned, error.clone());
            Err(WalWriteError::Failed(error))
        });
        (result, lease)
    }

    /// Serializes sequence reservation with the enqueue that follows it, so two appenders
    /// cannot interleave and hand the writer records out of sequence order.
    ///
    /// A panic while the lock is held poisons it, and leaves unknown whether the panicking
    /// append reserved a seq it never sent. Unwrapping the poison would make every later call
    /// on this handle panic too, including the ones that tear the session down. Instead the
    /// handle fails closed the way any other append failure does: it records a sticky writer
    /// poison before handing the guard back. From then on the writer task drops every append
    /// and every barrier reports the error, so nothing reserved in the unknown state can be
    /// written or claimed durable.
    #[cfg(not(test))]
    fn lock_enqueue(&self) -> std::sync::MutexGuard<'_, ()> {
        self.enqueue_lock
            .lock()
            .unwrap_or_else(|poisoned| self.recover_enqueue_lock(poisoned))
    }

    /// Fail the writer closed after a panic poisoned the enqueue lock, then keep using the
    /// lock: it guards no data, only the order of reservation and send.
    fn recover_enqueue_lock<'a>(
        &self,
        poisoned: std::sync::PoisonError<std::sync::MutexGuard<'a, ()>>,
    ) -> std::sync::MutexGuard<'a, ()> {
        set_poison(
            &self.poisoned,
            "an earlier WAL append panicked while holding the enqueue lock".into(),
        );
        poisoned.into_inner()
    }

    #[cfg(test)]
    fn lock_enqueue(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.enqueue_lock.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                let probe = self.enqueue_contention_probe.lock().unwrap().clone();
                if let Some(probe) = probe {
                    probe();
                }
                self.enqueue_lock
                    .lock()
                    .unwrap_or_else(|poisoned| self.recover_enqueue_lock(poisoned))
            }
            Err(std::sync::TryLockError::Poisoned(poisoned)) => self.recover_enqueue_lock(poisoned),
        }
    }

    #[cfg(test)]
    fn set_after_reserve_probe_for_test(&self, probe: AfterReserveProbe) {
        *self.after_reserve_probe.lock().unwrap() = Some(probe);
    }

    #[cfg(test)]
    fn run_after_reserve_probe(&self, seq: u64) {
        let probe = self.after_reserve_probe.lock().unwrap().clone();
        if let Some(probe) = probe {
            probe(seq);
        }
    }

    #[cfg(test)]
    fn set_enqueue_contention_probe_for_test(&self, probe: EnqueueContentionProbe) {
        *self.enqueue_contention_probe.lock().unwrap() = Some(probe);
    }

    #[cfg(test)]
    async fn stop_writer_for_test(&mut self) {
        let (ack, _result) = std_mpsc::channel();
        self.tx
            .send(WalCommand::Shutdown(ack))
            .expect("test writer is running");
        if let Some(join) = self.join.take() {
            tokio::task::spawn_blocking(move || join.join())
                .await
                .expect("test join task completes")
                .expect("test writer joins");
        }
    }
}

impl Drop for WalHandle {
    fn drop(&mut self) {
        if self.join.is_some() {
            if let Err(error) = self.shutdown_blocking() {
                tracing::error!("WAL writer shutdown failed during drop: {error}");
            }
        }
    }
}

type WriterSequence = (Arc<AtomicU64>, Arc<Mutex<()>>, Arc<AtomicU64>);

struct Publication {
    publisher: Option<broca_wal::head_proof::Publisher>,
    sequence: Option<WriterSequence>,
    deferred: std::collections::VecDeque<WalCommand>,
    fence: u64,
    clock: StampClock,
    observed: bool,
}

impl Publication {
    fn sync(
        &mut self,
        wal: &mut dyn Wal,
        rx: &std_mpsc::Receiver<WalCommand>,
        pending: &mut Vec<(u64, Box<WalRecord>)>,
    ) -> Result<(), String> {
        let Some(publisher) = &mut self.publisher else {
            return wal.sync().map_err(|error| error.to_string());
        };
        loop {
            publisher.sync(wal).map_err(|error| error.to_string())?;
            let now = self.clock.now_ms();
            let Some((through_seq, oldest_appended_ms)) = publisher.late(now) else {
                if publisher.publish(now).map_err(|error| error.to_string())? {
                    return Ok(());
                }
                continue;
            };
            let record = WalRecord::VisibilityLate {
                through_seq,
                oldest_appended_ms,
            };
            let prepared =
                broca_wal::PreparedV4Frame::prepare(&record).map_err(|error| error.to_string())?;
            let (next, enqueue, bytes) = self
                .sequence
                .as_ref()
                .expect("overhaul publication has a sequence allocator");
            // Reserve through the same lock as user appends. Requests already queued
            // keep their promised sequences; none can race into the marker's slot.
            let marker_seq = {
                let _guard = enqueue
                    .lock()
                    .map_err(|_| "visibility-late/v1: poisoned enqueue lock")?;
                bytes.fetch_add(
                    prepared.frame_len() as u64
                        + wal.append_budget().map_or(0, |(_, _, overhead)| overhead),
                    Ordering::SeqCst,
                );
                next.fetch_add(1, Ordering::SeqCst)
            };
            while wal.last_seq() < marker_seq - 1 {
                let command = rx
                    .recv()
                    .map_err(|_| "visibility-late/v1: missing queued append before marker")?;
                match command {
                    command @ (WalCommand::Append { .. } | WalCommand::AppendV4 { .. }) => {
                        let (seq, record) = append_command(wal, self.fence, &self.clock, command)?;
                        if self.observed {
                            pending.push((seq, record));
                        }
                    }
                    command => self.deferred.push_back(command),
                }
            }
            let seq = wal
                .append_v4(self.fence, &record, false, prepared, self.clock.now_ms())
                .map_err(|error| error.to_string())?;
            if seq != marker_seq {
                return Err("visibility-late/v1: marker sequence drift".into());
            }
            if self.observed {
                pending.push((seq, Box::new(record)));
            }
            // A second slow sync can age the marker too. Repeat, never publish
            // the late batch on the strength of a sync alone.
            crate::durability_counters::record_fsync(
                crate::durability_counters::FsyncCause::Barrier,
                self.fence,
            );
        }
    }
}

fn append_command(
    wal: &mut dyn Wal,
    fence: u64,
    clock: &StampClock,
    command: WalCommand,
) -> Result<(u64, Box<WalRecord>), String> {
    let (seq, record, result) = match command {
        WalCommand::Append { seq, record, frame } => {
            let result = match frame {
                Some(frame) => wal.append_preencoded(fence, &record, false, frame),
                None => wal.append(fence, &record, false),
            };
            (seq, record, result)
        }
        WalCommand::AppendV4 {
            seq,
            record,
            prepared,
        } => {
            let result = wal.append_v4(fence, &record, false, prepared, clock.now_ms());
            (seq, record, result)
        }
        _ => unreachable!("append_command accepts only append commands"),
    };
    match result {
        Ok(assigned) if assigned == seq => Ok((seq, record)),
        Ok(assigned) => Err(format!(
            "wal seq drift: handle assigned {seq}, wal assigned {assigned}"
        )),
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn writer_loop(
    wal: Box<dyn Wal + Send>,
    fence: u64,
    rx: std_mpsc::Receiver<WalCommand>,
    observer: Option<RecordObserver>,
    poisoned: Arc<Mutex<Option<String>>>,
    durable_head: Arc<AtomicU64>,
    lineage_id: Arc<Mutex<Option<broca_wal::WalLineageId>>>,
    stamp_clock: StampClock,
) {
    writer_loop_with_identity(
        wal,
        fence,
        rx,
        observer,
        poisoned,
        durable_head,
        lineage_id,
        stamp_clock,
        None,
        Publication {
            publisher: None,
            sequence: None,
            deferred: Default::default(),
            fence,
            clock: StampClock::wall(),
            observed: false,
        },
    );
}

#[allow(clippy::too_many_arguments)]
fn writer_loop_with_identity(
    mut wal: Box<dyn Wal + Send>,
    fence: u64,
    rx: std_mpsc::Receiver<WalCommand>,
    observer: Option<RecordObserver>,
    poisoned: Arc<Mutex<Option<String>>>,
    durable_head: Arc<AtomicU64>,
    lineage_id: Arc<Mutex<Option<broca_wal::WalLineageId>>>,
    stamp_clock: StampClock,
    writer: Option<open_writers::OpenWriter>,
    mut publication: Publication,
) {
    let mut writer = writer;
    use crate::durability_counters::{
        deadline_ms_for, monotonic_now_ms_for, record_fsync, FsyncCause,
    };

    // A control-stream cursor advances only after sync and publication. The WAL's
    // last_seq can include coalesced appends that a crash would lose. Startup has
    // already proved and published the recovered prefix behind writer_opened.
    let mut last_fsynced_seq = wal.last_seq();
    let mut dirty = false;
    // The sync deadline: the monotonic instant (in ms) by which dirty data must be
    // synced. Set when an append makes a clean writer dirty, and never slid while
    // dirty, so a queue that never empties cannot postpone the sync past it. Cleared
    // whenever the writer goes clean, and also when a sync fails: the writer is then
    // poisoned and must idle until close, not spin on a deadline that can never succeed.
    let mut deadline_at_ms: Option<u64> = None;
    let mut pending_observed: Vec<(u64, Box<WalRecord>)> = Vec::new();
    let has_tap = observer
        .as_ref()
        .is_some_and(|observer| observer.tap.is_some());

    // What the blocking wait below returned: a command, a bare timeout that
    // carried no command, or proof the handle is gone.
    enum Wait {
        Command(WalCommand),
        Empty,
        Gone,
    }

    loop {
        // One clock sample per dirty iteration; clean writers read no clock.
        // The deadline is checked before every command on that sample: at most
        // one command is handled past the deadline, so a queue that never
        // empties still syncs on time. The same sample bounds the wait, so a
        // dirty writer wakes at its deadline even when nothing arrives. A dirty
        // writer with no deadline is poisoned (its sync already failed), so it
        // idles like a clean one until close.
        let wait = if let Some(command) = publication.deferred.pop_front() {
            Wait::Command(command)
        } else if dirty {
            let now = monotonic_now_ms_for(fence);
            if deadline_at_ms.is_some_and(|deadline| now >= deadline) {
                let poison = current_poison(&poisoned);
                if poison.is_none() {
                    // Every fsync is counted by the path that issued it, for health.
                    record_fsync(FsyncCause::Deadline, fence);
                }
                match poison.map_or_else(
                    || publication.sync(&mut *wal, &rx, &mut pending_observed),
                    Err,
                ) {
                    Ok(()) => {
                        last_fsynced_seq = wal.last_seq();
                        durable_head.store(last_fsynced_seq, Ordering::Release);
                        dirty = false;
                        deadline_at_ms = None;
                        if let Some(obs) = &observer {
                            if has_tap {
                                for (seq, record) in &pending_observed {
                                    obs.observe(*seq, fence, record, wal.lineage_id());
                                }
                            } else {
                                for (seq, record) in pending_observed.drain(..) {
                                    obs.observe(seq, fence, &record, wal.lineage_id());
                                }
                            }
                        }
                        if has_tap {
                            if let Some(obs) = &observer {
                                for (seq, record) in pending_observed.drain(..) {
                                    obs.observe_tap(seq, fence, wal.lineage_id(), &record);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        set_poison(&poisoned, error);
                        deadline_at_ms = None;
                    }
                }
            }
            // Wait for the next command. The bound reuses the sample above: a
            // still-installed deadline postdates it by definition (a due one
            // just synced). A writer that just went clean or poisoned has no
            // deadline, so it blocks in `recv()` until a command arrives.
            if dirty {
                match deadline_at_ms {
                    Some(deadline) => {
                        match rx.recv_timeout(std::time::Duration::from_millis(
                            deadline.saturating_sub(now),
                        )) {
                            Ok(command) => Wait::Command(command),
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Wait::Empty,
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Wait::Gone,
                        }
                    }
                    None => match rx.recv() {
                        Ok(command) => Wait::Command(command),
                        Err(_) => Wait::Gone,
                    },
                }
            } else {
                match rx.recv() {
                    Ok(command) => Wait::Command(command),
                    Err(_) => Wait::Gone,
                }
            }
        } else {
            match rx.recv() {
                Ok(command) => Wait::Command(command),
                Err(_) => Wait::Gone,
            }
        };
        if matches!(wait, Wait::Gone) {
            // A dropped handle closes like Shutdown: sync dirty data before exit.
            if dirty {
                let poison = current_poison(&poisoned);
                if poison.is_none() {
                    // Every fsync is counted by the path that issued it, for health.
                    record_fsync(FsyncCause::Disconnect, fence);
                }
                match poison.map_or_else(
                    || publication.sync(&mut *wal, &rx, &mut pending_observed),
                    Err,
                ) {
                    Ok(()) => {
                        last_fsynced_seq = wal.last_seq();
                        durable_head.store(last_fsynced_seq, Ordering::Release);
                        if let Some(obs) = &observer {
                            for (seq, record) in pending_observed.drain(..) {
                                obs.observe(seq, fence, &record, wal.lineage_id());
                                if has_tap {
                                    obs.observe_tap(seq, fence, wal.lineage_id(), &record);
                                }
                            }
                        }
                    }
                    Err(error) => set_poison(&poisoned, error),
                }
            }
            if current_poison(&poisoned).is_none() {
                if let Err(error) = close_writer_identity(
                    &mut *wal,
                    writer.take(),
                    fence,
                    &stamp_clock,
                    observer.as_ref(),
                    &durable_head,
                    publication.publisher.as_mut(),
                ) {
                    set_poison(&poisoned, error);
                }
            }
            break;
        }
        // Group commit is unchanged: one sync still covers every append that
        // preceded it.
        let Wait::Command(cmd) = wait else {
            continue;
        };
        match cmd {
            #[cfg(test)]
            WalCommand::BudgetSnapshot(ack) => {
                let _ = ack.send(wal.append_budget());
            }
            #[cfg(test)]
            WalCommand::AppendDrained(ack) => {
                let _ = ack.send(());
            }
            command @ (WalCommand::Append { .. } | WalCommand::AppendV4 { .. }) => {
                if current_poison(&poisoned).is_none() {
                    match append_command(&mut *wal, fence, &stamp_clock, command) {
                        Ok((assigned, record)) => {
                            let became_dirty = !dirty;
                            dirty = true;
                            if became_dirty {
                                // The deadline starts when the writer goes dirty and
                                // never slides: later appends cannot postpone it.
                                deadline_at_ms = Some(
                                    monotonic_now_ms_for(fence)
                                        .saturating_add(deadline_ms_for(fence)),
                                );
                            }
                            // A legacy file mints its identity on this first append.
                            *lineage_id.lock().unwrap() = wal.lineage_id();
                            // Keep observer delivery behind the same durability barrier as the
                            // cursor. The writer preserves append order while one sync releases the
                            // whole buffered prefix to live subscribers.
                            if observer.is_some() {
                                pending_observed.push((assigned, record));
                            }
                        }
                        Err(error) => set_poison(&poisoned, error),
                    }
                }
            }
            WalCommand::Sync(ack) => {
                // When no new append has crossed the writer since the last successful
                // fsync, the current durable head is already exact — return it without
                // paying another no-op sync. If the writer is poisoned, still surface the
                // error even though the seqs happen to match.
                let poison = current_poison(&poisoned);
                let res = if dirty {
                    if poison.is_none() {
                        record_fsync(FsyncCause::Barrier, fence);
                    }
                    poison
                        .map_or_else(
                            || publication.sync(&mut *wal, &rx, &mut pending_observed),
                            Err,
                        )
                        .map(|()| {
                            last_fsynced_seq = wal.last_seq();
                            durable_head.store(last_fsynced_seq, Ordering::Release);
                            dirty = false;
                            deadline_at_ms = None;
                            if let Some(obs) = &observer {
                                if has_tap {
                                    for (seq, record) in &pending_observed {
                                        obs.observe(*seq, fence, record, wal.lineage_id());
                                    }
                                } else {
                                    for (seq, record) in pending_observed.drain(..) {
                                        obs.observe(seq, fence, &record, wal.lineage_id());
                                    }
                                }
                            }
                            last_fsynced_seq
                        })
                } else {
                    poison.map_or(Ok(last_fsynced_seq), Err)
                };
                let res = res.map_err(|e| {
                    set_poison(&poisoned, e.clone());
                    // The writer is poisoned now: drop the deadline with it, so
                    // the loop idles instead of retrying a sync that cannot succeed.
                    deadline_at_ms = None;
                    WalWriteError::Failed(e)
                });
                let durable = res.is_ok();
                let _ = ack.send(res);
                if durable && has_tap {
                    if let Some(obs) = &observer {
                        for (seq, record) in pending_observed.drain(..) {
                            obs.observe_tap(seq, fence, wal.lineage_id(), &record);
                        }
                    }
                }
            }
            WalCommand::Shutdown(ack) => {
                let poison = current_poison(&poisoned);
                let result = if dirty {
                    if poison.is_none() {
                        record_fsync(FsyncCause::Shutdown, fence);
                    }
                    poison.map_or_else(
                        || publication.sync(&mut *wal, &rx, &mut pending_observed),
                        Err,
                    )
                } else {
                    poison.map_or(Ok(()), Err)
                };
                if result.is_ok() && dirty {
                    durable_head.store(wal.last_seq(), Ordering::Release);
                    if let Some(obs) = &observer {
                        for (seq, record) in pending_observed.drain(..) {
                            obs.observe(seq, fence, &record, wal.lineage_id());
                            if has_tap {
                                obs.observe_tap(seq, fence, wal.lineage_id(), &record);
                            }
                        }
                    }
                }
                let result = result.and_then(|()| {
                    close_writer_identity(
                        &mut *wal,
                        writer.take(),
                        fence,
                        &stamp_clock,
                        observer.as_ref(),
                        &durable_head,
                        publication.publisher.as_mut(),
                    )
                });
                let result = result.map_err(|error| {
                    set_poison(&poisoned, error.clone());
                    WalWriteError::Failed(error)
                });
                let _ = ack.send(result);
                break;
            }
        }
    }
}

/// The message a panic carried, for the writer error that replaces it.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a non-string panic payload")
}

fn close_writer_identity(
    wal: &mut dyn Wal,
    writer: Option<open_writers::OpenWriter>,
    fence: u64,
    clock: &StampClock,
    observer: Option<&RecordObserver>,
    durable_head: &AtomicU64,
    publisher: Option<&mut broca_wal::head_proof::Publisher>,
) -> Result<(), String> {
    if let Some(writer) = writer {
        let (seq, record) = writer
            .close(wal, fence, clock.now_ms())
            .map_err(|error| format!("writer-id/v1: cannot close writer: {error}"))?;
        let published = if let Some(publisher) = publisher {
            publisher.sync(wal).map_err(|error| error.to_string())?;
            // Closing forbids further appends. If its sync was late, leave the
            // close unpublished for the next writer_opened rather than append again.
            publisher
                .publish(clock.now_ms())
                .map_err(|error| error.to_string())?
        } else {
            true
        };
        if published {
            durable_head.store(seq, Ordering::Release);
            if let Some(obs) = observer {
                obs.observe(seq, fence, &record, wal.lineage_id());
                obs.observe_tap(seq, fence, wal.lineage_id(), &record);
            }
        }
        if let Err(error) = writer.unlink() {
            tracing::warn!(%error, "writer-id/v1: durable writer close succeeded but marker unlink failed; reclaim will remove the marker");
        }
    }
    Ok(())
}

/// Records the first fatal writer error, which every later append and barrier then reports.
///
/// The `unwrap` on the lock is safe for a reason worth stating, because this is the mechanism
/// that records writer failure and a recorder that could itself fail would leave the writer
/// unable to report why it stopped: a `Mutex` is only poisoned by a panic while its guard is
/// held, and the guarded section here is an `is_none` test and a move. Neither can panic, so
/// this lock cannot be poisoned by its own critical section. Keep it that way — anything
/// fallible added inside the guard makes the failure recorder a second failure source.
fn set_poison(poisoned: &Mutex<Option<String>>, error: String) {
    let mut state = poisoned.lock().unwrap();
    if state.is_none() {
        *state = Some(error);
    }
}

/// Reads the recorded writer error. Guarded section is a single clone, so it cannot panic
/// while holding the guard and cannot poison this lock. See `set_poison` for why that matters.
fn current_poison(poisoned: &Mutex<Option<String>>) -> Option<String> {
    poisoned.lock().unwrap().clone()
}

#[cfg(test)]
mod tests {

    /// THE ADDRESS IS PINNED TO A LITERAL COMPUTED OUTSIDE THIS CODE.
    ///
    /// `session_addr` was extracted from `wal_path` on 2026-09-04 so a diagnostic could render
    /// the same handle the writer opens. That extraction is only safe if it is byte-identical:
    /// this hash IS the filename of every session's WAL, so a changed digest does not corrupt
    /// anything -- it silently addresses a file that does not exist, and 30,000 sessions read as
    /// empty rather than as an error.
    ///
    /// The expected value was computed in a separate Python implementation of FNV-1a, NOT by
    /// calling anything in this crate. A test that asserts a function against itself, or against
    /// a helper sharing its constants, passes cleanly through a coordinated change to both.
    #[test]
    fn session_addr_is_pinned_to_an_independently_computed_digest() {
        let session = BindIdentity::new("/tmp/p", "runner", "s1");
        assert_eq!(session_addr(&session), "31a7840439280f40");
    }

    /// And the address the writer OPENS is the address the diagnostic RENDERS.
    ///
    /// The pin above would still pass if `wal_path` stopped calling `session_addr` and kept its
    /// own copy -- which is the exact drift the extraction exists to prevent, and the state a
    /// reader would discover by following a logged handle to a file that is not there.
    #[test]
    fn wal_path_addresses_what_session_addr_renders() {
        let session = BindIdentity::new("/tmp/p", "runner", "s1");
        let store = FileWalStore::new("/tmp/wal-root");
        assert_eq!(
            store
                .wal_path(&session)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap(),
            format!("{}.wal", session_addr(&session))
        );
    }
    use super::*;
    use broca_protocol::StepId;

    fn tmp(tag: &str) -> PathBuf {
        broca_test_support::scratch_root().join(format!(
            "broca-walwriter-{}-{}",
            std::process::id(),
            tag
        ))
    }

    #[test]
    fn separator_collision_identities_share_the_legacy_wal_path_but_are_rejected() {
        let first = BindIdentity::new("/tmp/proj\u{1f}runner", "a", "b");
        let second = BindIdentity::new("/tmp/proj", "runner\u{1f}a", "b");
        let store = FileWalStore::new(tmp("identity-collision"));

        assert_ne!(first, second);
        assert_eq!(
            store.wal_path(&first),
            store.wal_path(&second),
            "the persisted delimiter encoding is intentionally unchanged"
        );
        assert!(first.validate_keyable().is_err());
        assert!(second.validate_keyable().is_err());
    }

    struct CountingWal {
        last_seq: u64,
        sync_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[test]
    fn idle_writers_leave_tiny_blocking_pool_available_for_jobs_and_dns() {
        const BLOCKING_THREADS: usize = 2;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(BLOCKING_THREADS)
            .build()
            .unwrap();
        runtime.block_on(async {
            let handles: Vec<_> = (0..BLOCKING_THREADS)
                .map(|_| {
                    WalHandle::spawn(
                        Box::new(CountingWal {
                            last_seq: 0,
                            sync_calls: Arc::new(AtomicUsize::new(0)),
                        }),
                        1,
                    )
                    .unwrap()
                })
                .collect();

            let bound = std::time::Duration::from_secs(2);
            // Spawning a thread does not mean its writer loop has started. Acknowledge
            // one barrier per pool slot before probing: if writers use the shared pool,
            // every slot is now occupied until shutdown, regardless of scheduling order.
            // More writers than slots would deadlock this readiness check in that case.
            let ready = tokio::time::timeout(bound, async {
                for handle in &handles {
                    handle.sync().await?;
                }
                Ok::<_, WalWriteError>(())
            })
            .await;
            let (job, dns) = tokio::join!(
                tokio::time::timeout(bound, tokio::task::spawn_blocking(|| 42)),
                tokio::time::timeout(bound, tokio::net::lookup_host("localhost:80")),
            );

            // Unpark every writer before asserting, including writers queued behind a
            // saturated pool in the regression. Runtime drop must not hang on a failed test.
            for handle in &handles {
                let (ack, _result) = std_mpsc::channel();
                let _ = handle.tx.send(WalCommand::Shutdown(ack));
            }
            for handle in handles {
                let _ = handle.shutdown().await;
            }

            ready
                .expect("WAL writers did not acknowledge startup barriers")
                .unwrap();
            assert_eq!(
                job.expect("idle WAL writers starved a finite blocking job")
                    .unwrap(),
                42
            );
            // Resolver errors depend on the host configuration (including sandboxed
            // runners). This guards completion of the DNS job, not resolver correctness.
            let _resolution = dns.expect("idle WAL writers starved localhost DNS resolution");
        });
    }

    impl Wal for CountingWal {
        fn append(
            &mut self,
            _fence: u64,
            _record: &WalRecord,
            sync: bool,
        ) -> Result<u64, WalError> {
            self.last_seq += 1;
            if sync {
                self.sync()?;
            }
            Ok(self.last_seq)
        }

        fn sync(&self) -> Result<(), WalError> {
            self.sync_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn tail_after_seq(
            &self,
            _after_seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn last_seq(&self) -> u64 {
            self.last_seq
        }
    }

    struct FailingSyncWal {
        last_seq: u64,
    }

    impl Wal for FailingSyncWal {
        fn append(
            &mut self,
            _fence: u64,
            _record: &WalRecord,
            _sync: bool,
        ) -> Result<u64, WalError> {
            self.last_seq += 1;
            Ok(self.last_seq)
        }

        fn sync(&self) -> Result<(), WalError> {
            Err(WalError::Io(std::io::Error::other(
                "injected recovery fsync failure",
            )))
        }

        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn tail_after_seq(
            &self,
            _after_seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn last_seq(&self) -> u64 {
            self.last_seq
        }
    }

    struct TrackingWal {
        last_seq: u64,
        appended: Arc<AtomicUsize>,
    }

    impl Wal for TrackingWal {
        fn append(
            &mut self,
            _fence: u64,
            _record: &WalRecord,
            sync: bool,
        ) -> Result<u64, WalError> {
            self.last_seq += 1;
            self.appended.fetch_add(1, Ordering::SeqCst);
            if sync {
                self.sync()?;
            }
            Ok(self.last_seq)
        }

        fn sync(&self) -> Result<(), WalError> {
            Ok(())
        }

        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn tail_after_seq(
            &self,
            _after_seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn last_seq(&self) -> u64 {
            self.last_seq
        }
    }

    /// A panic while an append holds the enqueue lock (here, after it reserved its seq and
    /// before it sent the record) fails the writer closed. Later calls on the handle get the
    /// writer error instead of panicking on the poisoned lock, so one bad record cannot turn
    /// every later append, barrier and teardown on the session into another panic.
    #[tokio::test]
    async fn a_panic_inside_an_append_fails_the_writer_closed_without_later_panics() {
        let dir = tmp("enqueue-panic");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileWalStore::new(&dir);
        let session = BindIdentity::new("/tmp/p", "h", "s");
        let handle = WalHandle::spawn(store.open(&session).unwrap(), 1).expect("spawn writer");
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        handle.set_after_reserve_probe_for_test(Arc::new(move |seq| {
            if first.swap(false, Ordering::SeqCst) {
                panic!("append {seq} panicked after reserving its seq");
            }
        }));
        let record = |step| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handle.record(WalRecord::StepStarted {
                    step_id: StepId(step),
                })
            }))
        };
        assert!(record(1).is_err(), "the probe panics the first append");
        assert!(
            record(2).is_ok(),
            "a later append panicked on the poisoned enqueue lock"
        );
        let error = handle.sync().await.expect_err("the writer failed closed");
        assert!(
            matches!(&error, WalWriteError::Failed(message)
                if message.contains("panicked while holding the enqueue lock")),
            "{error:?}"
        );
        assert!(handle.is_poisoned());
        assert!(handle.shutdown().await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn park_first_reservation(
        handle: &WalHandle,
    ) -> (std::sync::mpsc::Receiver<u64>, std::sync::mpsc::Sender<()>) {
        let (reserved_tx, reserved_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        handle.set_after_reserve_probe_for_test(Arc::new(move |seq| {
            if first.swap(false, Ordering::SeqCst) {
                reserved_tx
                    .send(seq)
                    .expect("test observes the first reserved sequence");
                release_rx
                    .lock()
                    .unwrap()
                    .recv()
                    .expect("test releases the parked record call");
            }
        }));
        (reserved_rx, release_tx)
    }

    #[tokio::test]
    async fn concurrent_records_preserve_reservation_order_on_disk() {
        #[derive(Debug)]
        enum RaceEvent {
            Contended,
            SecondReturned,
        }

        let dir = tmp("concurrent-record-order");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileWalStore::new(&dir);
        let session = BindIdentity::new("/tmp/p", "h", "s");
        let handle =
            Arc::new(WalHandle::spawn(store.open(&session).unwrap(), 1).expect("spawn writer"));
        let (reserved_rx, release_tx) = park_first_reservation(&handle);
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let contention_tx = event_tx.clone();
        handle.set_enqueue_contention_probe_for_test(Arc::new(move || {
            contention_tx
                .send(RaceEvent::Contended)
                .expect("test observes enqueue-lock contention");
        }));

        let first_handle = handle.clone();
        let first_thread = std::thread::spawn(move || {
            first_handle
                .record(WalRecord::StepStarted { step_id: StepId(1) })
                .into_result()
                .unwrap()
        });
        let reserved_seq = reserved_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("first record parks immediately after reserving its sequence");

        let second_handle = handle.clone();
        let second_thread = std::thread::spawn(move || {
            let seq = second_handle
                .record(WalRecord::StepStarted { step_id: StepId(2) })
                .into_result()
                .unwrap();
            event_tx
                .send(RaceEvent::SecondReturned)
                .expect("test observes the second record return");
            seq
        });

        // The first record holds the enqueue lock, so contention is the expected event. A
        // premature return also releases the parked call, allowing seq-drift to fail the test
        // instead of leaving both record threads deadlocked.
        let first_event = event_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("second record either contends or returns");
        release_tx
            .send(())
            .expect("release the first record after observing the race");
        let first_seq = first_thread.join().expect("first record thread joins");
        let second_seq = second_thread.join().expect("second record thread joins");
        assert_eq!((first_seq, second_seq), (reserved_seq, reserved_seq + 1));

        let head = handle
            .sync()
            .await
            .expect("FIFO append order must match reservation order without seq drift");
        assert!(
            matches!(first_event, RaceEvent::Contended),
            "the second record must wait behind the reserved first record"
        );
        assert_eq!(head, second_seq);

        let _ = Arc::try_unwrap(handle)
            .ok()
            .expect("record threads released their handles")
            .shutdown()
            .await;
        let durable = store.open(&session).unwrap().replay().unwrap();
        assert_eq!(durable.len(), 2);
        assert_eq!(durable[0].seq, first_seq);
        assert_eq!(
            durable[0].record,
            WalRecord::StepStarted { step_id: StepId(1) }
        );
        assert_eq!(durable[1].seq, second_seq);
        assert_eq!(
            durable[1].record,
            WalRecord::StepStarted { step_id: StepId(2) }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn sync_waits_for_an_already_reserved_append() {
        #[derive(Debug)]
        enum RaceEvent {
            Contended,
            SyncReturned,
        }

        let dir = tmp("sync-reserved-append");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileWalStore::new(&dir);
        let session = BindIdentity::new("/tmp/p", "h", "s");
        let handle =
            Arc::new(WalHandle::spawn(store.open(&session).unwrap(), 1).expect("spawn writer"));
        let (reserved_rx, release_tx) = park_first_reservation(&handle);
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let contention_tx = event_tx.clone();
        handle.set_enqueue_contention_probe_for_test(Arc::new(move || {
            contention_tx
                .send(RaceEvent::Contended)
                .expect("test observes enqueue-lock contention");
        }));

        let record_handle = handle.clone();
        let record_thread = std::thread::spawn(move || {
            record_handle
                .record(WalRecord::StepStarted { step_id: StepId(1) })
                .into_result()
                .unwrap()
        });
        let reserved_seq = reserved_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("record parks immediately after reserving its sequence");

        let sync_handle = handle.clone();
        let sync_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime builds");
            let result = runtime.block_on(sync_handle.sync());
            event_tx
                .send(RaceEvent::SyncReturned)
                .expect("test observes sync return");
            result
        });

        // A barrier queued before the parked append would acknowledge an incomplete durable
        // prefix. Waiting for contention or a premature return lets the test release the append
        // and report the ordering failure instead of deadlocking.
        let first_event = event_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("sync either contends or returns");
        release_tx
            .send(())
            .expect("release the reserved append after observing the race");
        let record_seq = record_thread.join().expect("record thread joins");
        let head = sync_thread
            .join()
            .expect("sync thread joins")
            .expect("sync barrier succeeds");
        assert!(
            matches!(first_event, RaceEvent::Contended),
            "sync must wait behind an append whose sequence is already reserved"
        );
        assert_eq!(record_seq, reserved_seq);
        assert_eq!(
            head, record_seq,
            "the durable head must include every sequence reserved before sync"
        );

        let _ = Arc::try_unwrap(handle)
            .ok()
            .expect("test threads released their handles")
            .shutdown()
            .await;
        let durable = store.open(&session).unwrap().replay().unwrap();
        assert_eq!(durable.len(), 1);
        assert_eq!(durable[0].seq, record_seq);
        assert_eq!(
            durable[0].record,
            WalRecord::StepStarted { step_id: StepId(1) }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn durable_tap_ignores_unsynced_appends_without_subscribers() {
        let appended = Arc::new(AtomicUsize::new(0));
        let (tx, _) = broadcast::channel(8);
        let (events, mut inbox) = tokio::sync::mpsc::channel(8);
        let observer = RecordObserver::new(tx, Arc::new(AtomicUsize::new(0))).with_durable_tap(
            Arc::new(move |seq, _, _, _| {
                events.try_send(seq).unwrap();
            }),
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        );
        let handle = WalHandle::spawn_observed(
            Box::new(TrackingWal {
                last_seq: 0,
                appended: appended.clone(),
            }),
            7,
            Some(observer),
        )
        .unwrap();
        handle.record(WalRecord::StepStarted { step_id: StepId(1) });
        while appended.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        // This FIFO probe acknowledges append processing without syncing it. Waiting only
        // on the append counter races a premature tap later in the same writer command.
        let (processed, ready) = oneshot::channel();
        assert!(handle.tx.send(WalCommand::AppendDrained(processed)).is_ok());
        ready.await.unwrap();
        assert!(
            matches!(
                inbox.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "unsynced append reached the tap"
        );
        handle.sync().await.unwrap();
        // The writer acknowledges the sync first and publishes to the tap afterwards, so the
        // run loop never waits on tap work; the event arrives shortly after the ack, not before.
        let event =
            broca_test_support::wait_for("the durable tap after sync", inbox.recv(), || {
                "no tap event after a successful sync".into()
            })
            .await
            .expect("tap channel open");
        let _ = handle.shutdown().await;
        assert_eq!(event, 1);
        assert!(
            inbox.try_recv().is_err(),
            "tap published the same append more than once"
        );
    }

    #[tokio::test]
    async fn panicking_durable_tap_marks_unknown_without_poisoning_writer() {
        let appended = Arc::new(AtomicUsize::new(0));
        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (tx, _) = broadcast::channel(8);
        let observer = RecordObserver::new(tx, Arc::new(AtomicUsize::new(0))).with_durable_tap(
            Arc::new(|_, _, _, _| panic!("injected tap failure")),
            healthy.clone(),
        );
        let handle = WalHandle::spawn_observed(
            Box::new(TrackingWal {
                last_seq: 0,
                appended: appended.clone(),
            }),
            7,
            Some(observer),
        )
        .unwrap();
        handle.record(WalRecord::StepStarted { step_id: StepId(1) });
        handle.sync().await.unwrap();
        handle.record(WalRecord::StepStarted { step_id: StepId(2) });
        assert_eq!(handle.sync().await.unwrap(), 2);
        let _ = handle.shutdown().await;
        assert!(
            !healthy.load(Ordering::Acquire),
            "a panicking tap still claims known state"
        );
        assert_eq!(appended.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn observer_queue_slots_hold_a_pointer_not_a_record() {
        // The fan-out's observer channel reserves all of its slots at actor open. A slot that
        // held the record by value would cost every open session 1,024 record-sized slots and
        // grow with every new WalRecord variant.
        assert_eq!(
            std::mem::size_of::<ObservedRecord>(),
            std::mem::size_of::<usize>()
        );
    }

    #[tokio::test]
    async fn observer_waits_for_sync_and_shutdown_durability_barriers() {
        let appended = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);
        let subscribers = Arc::new(AtomicUsize::new(1));
        let observer = RecordObserver::new(tx, subscribers);
        let handle = WalHandle::spawn_observed(
            Box::new(TrackingWal {
                last_seq: 0,
                appended: appended.clone(),
            }),
            7,
            Some(observer),
        )
        .expect("spawn writer");

        handle.record(WalRecord::StepStarted { step_id: StepId(1) });
        while appended.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "an appended record must remain invisible until its sync succeeds"
        );
        handle.sync().await.expect("first sync succeeds");
        assert_eq!(rx.recv().await.expect("synced record is published").seq, 1);

        handle.record(WalRecord::StepStarted { step_id: StepId(2) });
        while appended.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "the next appended record also waits for the shutdown sync"
        );
        let _ = handle.shutdown().await;
        assert_eq!(
            rx.recv()
                .await
                .expect("shutdown publishes after its final sync")
                .seq,
            2
        );
    }

    #[tokio::test]
    async fn sync_skips_clean_barriers_and_clean_shutdown() {
        let sync_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handle = WalHandle::spawn(
            Box::new(CountingWal {
                last_seq: 0,
                sync_calls: sync_calls.clone(),
            }),
            1,
        )
        .expect("spawn writer");

        assert_eq!(
            handle.sync().await.unwrap(),
            0,
            "empty sync returns the durable head"
        );
        assert_eq!(
            sync_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the spawn recovery barrier is the only sync before a clean barrier"
        );

        handle.record(WalRecord::TurnFinished { step_id: StepId(1) });
        assert_eq!(
            handle.sync().await.unwrap(),
            1,
            "the dirty barrier advances the head"
        );
        assert_eq!(
            sync_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the first dirty barrier follows the one recovery sync"
        );

        assert_eq!(
            handle.sync().await.unwrap(),
            1,
            "the durable head stays put without new appends"
        );
        let _ = handle.shutdown().await;
        assert_eq!(
            sync_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a clean shutdown must not pay a redundant final sync"
        );
    }

    #[test]
    fn spawn_refuses_to_publish_an_unproven_recovery_tail() {
        let error = match WalHandle::spawn(Box::new(FailingSyncWal { last_seq: 7 }), 1) {
            Ok(_) => panic!("a failed recovery fsync must refuse to construct a writer handle"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("injected recovery fsync failure"),
            "the recovery failure must be surfaced to the opener: {error}"
        );
    }

    #[tokio::test]
    async fn spawn_proves_an_inherited_prefix_before_exposing_its_head() {
        let sync_calls = Arc::new(AtomicUsize::new(0));
        let handle = WalHandle::spawn(
            Box::new(CountingWal {
                last_seq: 7,
                sync_calls: sync_calls.clone(),
            }),
            1,
        )
        .expect("recovery fsync proves the inherited prefix");
        assert_eq!(
            handle.durable_head().load(Ordering::Acquire),
            7,
            "the inherited head is exposed only after the recovery barrier succeeds"
        );
        assert_eq!(sync_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handle.sync().await.expect("clean writer stays proven"), 7);
        assert_eq!(
            sync_calls.load(Ordering::SeqCst),
            1,
            "the clean barrier reuses the established recovery proof"
        );
        let _ = handle.shutdown().await;
    }

    #[tokio::test]
    async fn failed_append_send_poisons_the_next_barrier() {
        let sync_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handle = WalHandle::spawn(
            Box::new(CountingWal {
                last_seq: 0,
                sync_calls,
            }),
            1,
        )
        .expect("spawn writer");
        handle.stop_writer_for_test().await;

        assert!(matches!(
            handle.record(WalRecord::TurnFinished { step_id: StepId(1) }),
            RecordOutcome::Refused(_)
        ));
        let error = handle
            .sync()
            .await
            .expect_err("a barrier after a dropped append must fail");

        assert!(
            error.to_string().contains("append command for seq 1"),
            "the failed append poisons the handle before sync: {error}"
        );
    }

    #[tokio::test]
    async fn shutdown_flushes_dirty_records_once() {
        let sync_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handle = WalHandle::spawn(
            Box::new(CountingWal {
                last_seq: 0,
                sync_calls: sync_calls.clone(),
            }),
            1,
        )
        .expect("spawn writer");

        handle.record(WalRecord::TurnFinished { step_id: StepId(1) });
        let _ = handle.shutdown().await;
        assert_eq!(
            sync_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "shutdown flushes dirty appends after the recovery barrier"
        );
    }

    #[tokio::test]
    async fn records_then_sync_are_durable_and_shutdown_flushes() {
        let dir = tmp("durable");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileWalStore::new(&dir);
        let session = BindIdentity::new("/tmp/p", "h", "s");
        let wal = store.open(&session).unwrap();

        let handle = WalHandle::spawn(wal, 1).expect("spawn writer");
        handle.record(WalRecord::TurnFinished { step_id: StepId(2) });
        handle.record(WalRecord::TurnFinished { step_id: StepId(1) });
        let head = handle.sync().await.expect("sync ok");
        assert_eq!(
            head, 2,
            "sync returns the last fsynced seq (2 records, 1-based)"
        );
        let _ = handle.shutdown().await;

        // Reopen and confirm both records are durable.
        let wal2 = store.open(&session).unwrap();
        assert_eq!(wal2.replay().unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn record_returns_the_final_monotonic_seq_matching_the_durable_record() {
        // The cursor-at-publish contract: record() returns the seq the record WILL have,
        // matching the seq it ends up with on disk (so a fan-out ring entry tagged with
        // record()'s return value has the SAME cursor the projection will assign). Seqs
        // are 1-based and monotonic; the returned seq never needs a backpatch.
        let dir = tmp("record-seq");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileWalStore::new(&dir);
        let session = BindIdentity::new("/tmp/p", "h", "s");

        let handle = WalHandle::spawn(store.open(&session).unwrap(), 1).expect("spawn writer");
        let s1 = handle
            .record(WalRecord::StepStarted { step_id: StepId(1) })
            .into_result()
            .unwrap();
        let s2 = handle
            .record(WalRecord::StepStarted { step_id: StepId(2) })
            .into_result()
            .unwrap();
        let s3 = handle
            .record(WalRecord::TurnFinished { step_id: StepId(2) })
            .into_result()
            .unwrap();
        assert_eq!(
            (s1, s2, s3),
            (1, 2, 3),
            "record() returns 1-based monotonic seqs"
        );
        handle.sync().await.expect("sync ok");
        let _ = handle.shutdown().await;

        // The durable records carry EXACTLY the seqs record() returned — no drift, so a
        // ring cursor tagged at publish equals the projection's cursor.
        let durable = store.open(&session).unwrap().replay().unwrap();
        let seqs: Vec<u64> = durable.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![s1, s2, s3]);
    }

    /// A real file log that reports no identity until its first append, the way a legacy
    /// WAL (written before lineage ids existed) behaves.
    struct IdentityOnFirstAppend {
        inner: broca_wal::FileWal,
        appended: bool,
    }

    impl Wal for IdentityOnFirstAppend {
        fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
            let seq = self.inner.append(fence, record, sync)?;
            self.appended = true;
            Ok(seq)
        }
        fn sync(&self) -> Result<(), WalError> {
            self.inner.sync()
        }
        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            self.inner.replay()
        }
        fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            self.inner.tail_after_seq(after_seq)
        }
        fn last_seq(&self) -> u64 {
            self.inner.last_seq()
        }
        fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
            self.appended.then(|| self.inner.lineage_id()).flatten()
        }
    }

    #[tokio::test]
    async fn handle_reports_a_lineage_minted_by_a_later_append() {
        let path = tmp("lineage-on-first-append.wal");
        let _ = std::fs::remove_file(&path);
        let inner = broca_wal::FileWal::open(&path).unwrap();
        let on_disk = inner.lineage_id();
        let handle = WalHandle::spawn(
            Box::new(IdentityOnFirstAppend {
                inner,
                appended: false,
            }),
            1,
        )
        .expect("spawn writer");
        assert_eq!(handle.lineage_id(), None, "nothing appended yet");
        handle.record(WalRecord::StepStarted { step_id: StepId(1) });
        handle.sync().await.expect("sync ok");
        assert_eq!(
            handle.lineage_id(),
            on_disk,
            "after a barrier the handle reports the identity the append minted"
        );
        let _ = handle.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn record_seq_continues_past_a_preexisting_prefix() {
        // record()'s assigned seq accounts for an existing durable prefix (the counter is
        // seeded from the WAL's last_seq at spawn) — so a resumed run's ring cursors don't
        // collide with the prefix's.
        let dir = tmp("record-seq-prefix");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileWalStore::new(&dir);
        let session = BindIdentity::new("/tmp/p", "h", "s");
        {
            let h = WalHandle::spawn(store.open(&session).unwrap(), 1).expect("spawn writer");
            assert_eq!(
                h.record(WalRecord::StepStarted { step_id: StepId(1) })
                    .into_result()
                    .unwrap(),
                1
            );
            h.sync().await.unwrap();
            let _ = h.shutdown().await;
        }
        // Reopen: one record (seq 1) durable → the next record() returns 2.
        let h = WalHandle::spawn(store.open(&session).unwrap(), 2).expect("spawn writer");
        assert_eq!(
            h.record(WalRecord::StepStarted { step_id: StepId(2) })
                .into_result()
                .unwrap(),
            2,
            "record() continues from the durable prefix, not from 1"
        );
        h.sync().await.unwrap();
        let _ = h.shutdown().await;
    }

    #[tokio::test]
    async fn sync_seq_accounts_for_a_preexisting_prefix() {
        // A writer over a WAL that already has durable records reports a fsynced seq
        // that includes them (the cursor must not rewind past the pre-existing prefix).
        let dir = tmp("prefix");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileWalStore::new(&dir);
        let session = BindIdentity::new("/tmp/p", "h", "s");
        {
            let handle = WalHandle::spawn(store.open(&session).unwrap(), 1).expect("spawn writer");
            handle.record(WalRecord::TurnFinished { step_id: StepId(1) });
            handle.sync().await.unwrap();
            let _ = handle.shutdown().await;
        }
        // Reopen: one record (seq 1) is already durable.
        let handle = WalHandle::spawn(store.open(&session).unwrap(), 2).expect("spawn writer");
        let head = handle.sync().await.expect("sync ok");
        assert_eq!(
            head, 1,
            "with nothing newly appended, the head is the durable prefix's last seq"
        );
        handle.record(WalRecord::TurnFinished { step_id: StepId(2) });
        let head2 = handle.sync().await.expect("sync ok");
        assert_eq!(head2, 2, "a new append advances the head past the prefix");
        let _ = handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disabled_observer_skips_publish_until_a_subscriber_enables_it() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);
        let subscribers = Arc::new(AtomicUsize::new(0));
        let observer = RecordObserver::new(tx, subscribers.clone());

        observer.observe(1, 7, &WalRecord::StepStarted { step_id: StepId(1) }, None);
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "with no subscribers, the writer must not clone records into the observer channel"
        );

        subscribers.fetch_add(1, Ordering::AcqRel);
        observer.observe(2, 7, &WalRecord::StepStarted { step_id: StepId(2) }, None);
        let framed = rx
            .try_recv()
            .expect("once a subscriber enables the gate, records must publish live");
        assert_eq!(framed.seq, 2);
        assert_eq!(framed.fence, 7);
        assert!(matches!(
            framed.record,
            WalRecord::StepStarted { step_id: StepId(2) }
        ));
    }

    #[test]
    fn observer_queue_is_bounded_and_reports_dropped_oldest_records() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(2);
        let subscribers = Arc::new(AtomicUsize::new(1));
        let observer = RecordObserver::new(tx, subscribers);

        for seq in 1..=3 {
            observer.observe(
                seq,
                7,
                &WalRecord::StepStarted {
                    step_id: StepId(seq),
                },
                None,
            );
        }

        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(1))
        ));
        assert_eq!(rx.try_recv().expect("oldest retained record").seq, 2);
        assert_eq!(rx.try_recv().expect("newest retained record").seq, 3);
    }
}

#[cfg(test)]
mod archive_tests;

#[cfg(test)]
mod deadline_tests;

#[cfg(test)]
mod shutdown_tests;

#[cfg(test)]
pub(crate) mod retention_tests;

#[cfg(test)]
mod session_class_tests;

#[cfg(test)]
mod stamp_tests;

#[cfg(test)]
mod tool_times_tests;

#[cfg(test)]
mod writer_id_tests;

#[cfg(test)]
mod head_proof_tests {
    use super::*;
    use broca_wal::head_proof::{sidecar_path, HeadProof, PublishedLog};

    struct Root(PathBuf);
    impl Root {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = broca_test_support::scratch_root().join(format!(
                "head-proof-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn store(&self) -> FileWalStore {
            FileWalStore::with_config(self.0.join("wal"), broca_wal::WalConfig::OVERHAUL)
        }
    }
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct StalledWal {
        inner: Box<dyn Wal + Send>,
        now: Arc<AtomicU64>,
        stalls: Arc<AtomicU64>,
        gate: Option<(std_mpsc::Sender<()>, Mutex<std_mpsc::Receiver<()>>)>,
        path_access: PathAccess,
    }
    enum PathAccess {
        Forward,
        Missing,
        Forbidden,
    }
    impl Wal for StalledWal {
        fn log_path(&self) -> Option<&Path> {
            match self.path_access {
                PathAccess::Forward => self.inner.log_path(),
                PathAccess::Missing => None,
                PathAccess::Forbidden => panic!("legacy writer must never request a proof path"),
            }
        }
        fn wal_config(&self) -> broca_wal::WalConfig {
            self.inner.wal_config()
        }
        fn writer_id(&self) -> Option<[u8; 16]> {
            self.inner.writer_id()
        }
        fn writer_close_reserve(&self) -> u64 {
            self.inner.writer_close_reserve()
        }
        fn append_budget(&self) -> Option<(u64, u64, u64)> {
            self.inner.append_budget()
        }
        fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
            self.inner.append(fence, record, sync)
        }
        fn append_v4(
            &mut self,
            fence: u64,
            record: &WalRecord,
            sync: bool,
            prepared: broca_wal::PreparedV4Frame,
            at: u64,
        ) -> Result<u64, WalError> {
            self.inner.append_v4(fence, record, sync, prepared, at)
        }
        fn sync(&self) -> Result<(), WalError> {
            self.inner.sync()?;
            let stalls = self.stalls.load(Ordering::SeqCst);
            if stalls > 0 {
                self.stalls.store(stalls - 1, Ordering::SeqCst);
                self.now.fetch_add(70_000, Ordering::SeqCst);
                if let Some((entered, release)) = &self.gate {
                    entered.send(()).unwrap();
                    release.lock().unwrap().recv().unwrap();
                }
            }
            Ok(())
        }
        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            self.inner.replay()
        }
        fn tail_after_seq(&self, seq: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            self.inner.tail_after_seq(seq)
        }
        fn last_seq(&self) -> u64 {
            self.inner.last_seq()
        }
        fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
            self.inner.lineage_id()
        }
    }

    fn ordinary() -> WalRecord {
        WalRecord::TurnFinished {
            step_id: broca_protocol::StepId(1),
        }
    }

    #[test]
    fn pathless_overhaul_open_refuses_before_spawning_or_writing() {
        let root = Root::new();
        let store = root.store();
        let session = BindIdentity::new("/proof", "runner", "pathless");
        let inner = store.open(&session).unwrap();
        let path = store.wal_path(&session);
        let before = std::fs::read(&path).unwrap();
        let wal = StalledWal {
            inner,
            now: Arc::new(AtomicU64::new(1000)),
            stalls: Arc::new(AtomicU64::new(0)),
            gate: None,
            path_access: PathAccess::Missing,
        };
        let result = WalHandle::spawn_proven_with_clock(
            Box::new(wal),
            0,
            1,
            None,
            None,
            (
                StampClock::wall(),
                Some(open_writers::WriterContext {
                    root: root.0.clone(),
                    address: session_addr(&session),
                }),
            ),
        );
        assert!(matches!(result, Err(WalWriteError::ProofPathUnavailable)));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(!root.0.join("open-writers").exists());
        assert!(!sidecar_path(&path).exists());
    }

    #[tokio::test]
    async fn legacy_writer_never_requests_a_proof_path() {
        let root = Root::new();
        let store = FileWalStore::new(root.0.join("wal"));
        let session = BindIdentity::new("/proof", "runner", "legacy-pathless");
        let wal = StalledWal {
            inner: store.open(&session).unwrap(),
            now: Arc::new(AtomicU64::new(1000)),
            stalls: Arc::new(AtomicU64::new(0)),
            gate: None,
            path_access: PathAccess::Forbidden,
        };
        let reads = Arc::new(AtomicU64::new(0));
        let clock = {
            let reads = reads.clone();
            StampClock::from_fn(move || {
                reads.fetch_add(1, Ordering::SeqCst);
                1000
            })
        };
        let handle =
            WalHandle::spawn_proven_with_clock(Box::new(wal), 0, 1, None, None, (clock, None))
                .unwrap();
        handle.record(ordinary()).into_result().unwrap();
        handle.sync().await.unwrap();
        handle.shutdown().await.unwrap();
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "legacy append, sync and close do not sample the stamp clock"
        );
        assert!(!sidecar_path(&store.wal_path(&session)).exists());
        assert!(!root.0.join("open-writers").exists());
    }

    fn timed_handle(
        root: &Root,
        session: &BindIdentity,
        now: Arc<AtomicU64>,
        stalls: Arc<AtomicU64>,
    ) -> WalHandle {
        let wal = StalledWal {
            inner: root.store().open(session).unwrap(),
            now: now.clone(),
            stalls,
            gate: None,
            path_access: PathAccess::Forward,
        };
        let clock = StampClock::from_fn(move || now.load(Ordering::SeqCst));
        WalHandle::spawn_proven_with_clock(
            Box::new(wal),
            0,
            1,
            None,
            None,
            (
                clock,
                Some(open_writers::WriterContext {
                    root: root.0.clone(),
                    address: session_addr(session),
                }),
            ),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn late_marker_is_rechecked_after_another_stalled_sync() {
        let root = Root::new();
        let session = BindIdentity::new("/proof", "runner", "repeated-stall");
        let now = Arc::new(AtomicU64::new(1000));
        let stalls = Arc::new(AtomicU64::new(0));
        let handle = timed_handle(&root, &session, now.clone(), stalls.clone());
        handle.record(ordinary()).into_result().unwrap();
        stalls.store(2, Ordering::SeqCst);
        assert_eq!(handle.sync().await.unwrap(), 4);
        let records = root.store().read_records(&session).unwrap();
        assert!(matches!(
            records[2].record,
            WalRecord::VisibilityLate { through_seq: 2, .. }
        ));
        assert!(matches!(
            records[3].record,
            WalRecord::VisibilityLate { through_seq: 3, .. }
        ));
        let proof = HeadProof::decode(
            &std::fs::read(sidecar_path(&root.store().wal_path(&session))).unwrap(),
        )
        .unwrap();
        assert_eq!(proof.durable_seq, 4);
        assert_eq!(proof.appended_ms, 141_000);
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn late_close_is_left_for_the_next_writer_opened() {
        let root = Root::new();
        let session = BindIdentity::new("/proof", "runner", "late-close");
        let now = Arc::new(AtomicU64::new(1000));
        let stalls = Arc::new(AtomicU64::new(0));
        let handle = timed_handle(&root, &session, now, stalls.clone());
        let head = handle.durable_head();
        stalls.store(1, Ordering::SeqCst);
        handle.shutdown().await.unwrap();
        let path = root.store().wal_path(&session);
        let proof = HeadProof::decode(&std::fs::read(sidecar_path(&path)).unwrap()).unwrap();
        assert_eq!(proof.durable_seq, 1);
        assert_eq!(head.load(Ordering::Acquire), 1);
        assert!(matches!(
            root.store()
                .read_records(&session)
                .unwrap()
                .last()
                .unwrap()
                .record,
            WalRecord::WriterClosed { .. }
        ));
        let store = root.store();
        let next =
            WalHandle::spawn_in_store(store.open(&session).unwrap(), &store, &session, 2, None)
                .unwrap();
        assert_eq!(
            HeadProof::decode(&std::fs::read(sidecar_path(&path)).unwrap())
                .unwrap()
                .durable_seq,
            3
        );
        next.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn late_batch_published_behind_marker() {
        let root = Root::new();
        let store = root.store();
        let session = BindIdentity::new("/proof", "runner", "session");
        let path = store.wal_path(&session);
        let now = Arc::new(AtomicU64::new(1000));
        let stalls = Arc::new(AtomicU64::new(0));
        let (entered_tx, entered_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let wal = StalledWal {
            inner: store.open(&session).unwrap(),
            now: now.clone(),
            stalls: stalls.clone(),
            gate: Some((entered_tx, Mutex::new(release_rx))),
            path_access: PathAccess::Forward,
        };
        let clock = {
            let now = now.clone();
            StampClock::from_fn(move || now.load(Ordering::SeqCst))
        };
        let handle = WalHandle::spawn_proven_with_clock(
            Box::new(wal),
            0,
            1,
            None,
            None,
            (
                clock,
                Some(open_writers::WriterContext {
                    root: root.0.clone(),
                    address: session_addr(&session),
                }),
            ),
        )
        .unwrap();
        assert_eq!(
            HeadProof::decode(&std::fs::read(sidecar_path(&path)).unwrap())
                .unwrap()
                .durable_seq,
            1
        );
        assert_eq!(handle.record(ordinary()).into_result().unwrap(), 2);
        stalls.store(1, Ordering::SeqCst);
        let (ack_tx, ack_rx) = oneshot::channel();
        handle.tx.send(WalCommand::Sync(ack_tx)).unwrap();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            HeadProof::decode(&std::fs::read(sidecar_path(&path)).unwrap())
                .unwrap()
                .durable_seq,
            1
        );
        // An append and another barrier queued during the stalled sync must keep
        // their reserved sequences when publication inserts its late marker.
        assert_eq!(handle.record(ordinary()).into_result().unwrap(), 3);
        let (later_tx, later_rx) = oneshot::channel();
        handle.tx.send(WalCommand::Sync(later_tx)).unwrap();
        release_tx.send(()).unwrap();
        assert_eq!(ack_rx.await.unwrap().unwrap(), 4);
        assert_eq!(later_rx.await.unwrap().unwrap(), 4);
        let bytes = std::fs::read(&path).unwrap();
        let sidecar = std::fs::read(sidecar_path(&path)).unwrap();
        let published = PublishedLog::open(&bytes, &sidecar).unwrap();
        assert_eq!(published.head().durable_seq, 4);
        assert_eq!(published.head().appended_ms, 71_000);
        let records = store.read_records(&session).unwrap();
        assert!(matches!(
            records.last().unwrap().record,
            WalRecord::VisibilityLate {
                through_seq: 2,
                oldest_appended_ms: 1000
            }
        ));
        assert_eq!(handle.record(ordinary()).into_result().unwrap(), 5);
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_publishes_behind_writer_opened() {
        let root = Root::new();
        let store = root.store();
        let session = BindIdentity::new("/proof", "runner", "recovery");
        let mut wal = store.open(&session).unwrap();
        wal.append_v4(
            1,
            &ordinary(),
            false,
            broca_wal::PreparedV4Frame::prepare(&ordinary()).unwrap(),
            1,
        )
        .unwrap();
        drop(wal);
        let path = store.wal_path(&session);
        assert!(!sidecar_path(&path).exists());
        let handle =
            WalHandle::spawn_in_store(store.open(&session).unwrap(), &store, &session, 2, None)
                .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let sidecar = std::fs::read(sidecar_path(&path)).unwrap();
        let published = PublishedLog::open(&bytes, &sidecar).unwrap();
        assert_eq!(published.head().durable_seq, 2);
        assert!(published.head().appended_ms > 1);
        assert!(matches!(
            store.read_records(&session).unwrap().last().unwrap().record,
            WalRecord::WriterOpened {
                recovered_through_seq: 1,
                ..
            }
        ));
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn proof_and_log_path_follow_first_send_relocation() {
        let root = Root::new();
        let store = root.store();
        let session = BindIdentity::new("/proof", "runner", "placement");
        let source = store.wal_path(&session);
        let handle =
            WalHandle::spawn_in_store(store.open(&session).unwrap(), &store, &session, 1, None)
                .unwrap();
        handle.shutdown().await.unwrap();
        assert!(sidecar_path(&source).exists());
        let destination = root.0.join("wal-ephemeral/placed.wal");
        let mut wal = FileWal::open(&source)
            .unwrap()
            .with_wal_config(broca_wal::WalConfig::OVERHAUL)
            .unwrap();
        assert_eq!(wal.log_path(), Some(source.as_path()));
        wal.relocate_unstarted(&destination).unwrap();
        assert_eq!(wal.log_path(), Some(destination.as_path()));
        assert!(!source.exists());
        assert!(!sidecar_path(&source).exists());
        let bytes = std::fs::read(&destination).unwrap();
        let sidecar = std::fs::read(sidecar_path(&destination)).unwrap();
        assert!(PublishedLog::open(&bytes, &sidecar).is_some());
    }
}
