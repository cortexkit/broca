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

// Carry the caller's WAL access-counter context into blocking recovery work so
// startup reads stay attributed when execution moves to another thread.

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

use crate::lease::LeaseHandle;

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
pub use projection_gap::ProjectionAdmission;
pub use projection_gap::{
    session_projection_incomplete, ProjectionRepair, SESSION_PROJECTION_INCOMPLETE,
};
mod session_index;
pub use session_index::{agent_session_ref, legacy_session_ref, SessionRefIndex};

/// A writer opened under a session lease, and the lease its caller keeps.
pub type WriterAndLease = (Box<dyn Wal + Send>, Box<dyn LeaseHandle>);

/// Opens the per-session WAL (mirrors `LeaseStore`, so the file impl swaps for a
/// future store without touching the loop).
pub trait WalStore: Send + Sync {
    fn served_history(&self) -> Option<Arc<dyn crate::ServedHistory>> {
        None
    }

    fn bind_recovery_projection(
        &self,
        _session: Option<&BindIdentity>,
        _store: Arc<dyn crate::SessionStore>,
    ) {
    }
    /// Called only under the writer lease, before retention/admission read the
    /// physical log. Read-only store operations never initiate repair.
    fn recover_existing(&self, _session: &BindIdentity) -> Result<(), WalError> {
        Ok(())
    }
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
    /// How a reader that doesn't go through [`Self::check_move`] refuses `session` when the new
    /// session storage owns it, or a move into it is in flight (see
    /// [`FileWalStore::store_owner_refusal`]). `None` when today's storage owns the session,
    /// which is always so for a store with no state root.
    fn store_owner_refusal(&self, _session: &BindIdentity) -> Result<Option<WalError>, WalError> {
        Ok(None)
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

    /// Preserve a failed location probe's typed cause for admission checks.
    /// Stores with no fallible probe keep their explicit known/unknown answer.
    fn wal_present_checked(&self, session: &BindIdentity) -> Result<Option<bool>, WalError> {
        Ok(self.wal_present(session))
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

    /// Extend an already verified media seed. The read budget may be spent on the
    /// suffix rather than on rereading its captured historical prefix.
    fn read_media_seed_suffix(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: u64,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.read_records_range(session, after_seq, Some(through_seq), anchor)
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
    served_history: Mutex<Option<Arc<dyn crate::ServedHistory>>>,
    recovery_views: Mutex<RecoveryViews>,
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
    publication_observer: Option<Arc<PublicationObserverFactory>>,
    session_guard: Option<Arc<SessionGuard>>,
    #[cfg(any(test, feature = "test-clock"))]
    range_reads: AtomicU64,
}

#[derive(Default)]
struct RecoveryViews {
    global: Vec<std::sync::Weak<dyn crate::SessionStore>>,
    sessions: std::collections::HashMap<String, Vec<std::sync::Weak<dyn crate::SessionStore>>>,
}

impl RecoveryViews {
    fn snapshot(&self, session: &BindIdentity) -> Vec<Arc<dyn crate::SessionStore>> {
        let mut views = Vec::new();
        for view in self.global.iter().chain(
            self.sessions
                .get(&session_addr(session))
                .into_iter()
                .flatten(),
        ) {
            if let Some(view) = view.upgrade() {
                if !views.iter().any(|old| Arc::ptr_eq(old, &view)) {
                    views.push(view);
                }
            }
        }
        views
    }

    fn bind(&mut self, session: Option<&BindIdentity>, store: Arc<dyn crate::SessionStore>) {
        let list = match session {
            Some(session) => self.sessions.entry(session_addr(session)).or_default(),
            None => &mut self.global,
        };
        list.retain(|view| view.strong_count() > 0);
        let view = Arc::downgrade(&store);
        if !list.iter().any(|old| old.ptr_eq(&view)) {
            list.push(view);
        }
    }
}

struct ProjectionRecovery {
    history: Option<Arc<dyn crate::ServedHistory>>,
    session: BindIdentity,
    views: Vec<Arc<dyn crate::SessionStore>>,
    max_bytes: u64,
}

impl ProjectionRecovery {
    fn failed(&self, stage: &str, error: impl std::fmt::Display) -> WalError {
        WalError::Io(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "WAL recovery {stage} for session {} ({}): {error}",
                self.session.session,
                agent_session_ref(&self.session)
            ),
        ))
    }
}

impl broca_wal::recovery::RecoveryHooks for ProjectionRecovery {
    fn before_cut(&self, cut: &broca_wal::recovery::RecoveryCut) -> Result<(), WalError> {
        self.before_cut_with_lineage(cut, None)
    }

    fn before_cut_with_lineage(
        &self,
        cut: &broca_wal::recovery::RecoveryCut,
        lineage: Option<broca_wal::WalLineageId>,
    ) -> Result<(), WalError> {
        let lineage = lineage.map(|id| id.to_string());
        if let Some(history) = &self.history {
            history
                .history_rewritten(&self.session, lineage.as_deref())
                .map_err(|error| self.failed("history fence refused before cut", error))?;
        } else {
            for view in &self.views {
                if let Some(history) = view.served_history() {
                    history
                        .history_rewritten(&self.session, lineage.as_deref())
                        .map_err(|error| self.failed("history fence refused before cut", error))?;
                }
            }
        }
        for view in &self.views {
            view.invalidate_wal_recovery(&self.session, cut)
                .map_err(|e| self.failed("invalidation refused before cut", e))?;
        }
        Ok(())
    }
    fn after_repair(
        &self,
        log: &Path,
        cut_ids: &std::collections::BTreeSet<String>,
    ) -> Result<(), WalError> {
        let mut full = None;
        for view in &self.views {
            if view
                .needs_wal_recovery_projection(&self.session, cut_ids)
                .map_err(|e| self.failed("projection check", e))?
            {
                if full.is_none() {
                    full = Some(
                        broca_wal::file_wal::read_records_range(log, self.max_bytes, 0, None, None)
                            .map_err(|e| self.failed("projection WAL read", e))?,
                    );
                }
                view.reproject_wal_recovery(&self.session, full.as_ref().unwrap())
                    .map_err(|e| self.failed("reprojection refused after cut", e))?;
            }
        }
        Ok(())
    }
}

/// Non-I/O notification of a successful WAL barrier, including raw maintenance writers.
/// The callback must retain only the session identity and durable head, not record bodies.
pub type DurableWalObserver =
    dyn Fn(&BindIdentity, u64, Option<broca_wal::WalLineageId>) + Send + Sync;

/// Installed at open, away from barriers. Its returned callback must perform no
/// I/O or blocking work; it only records the writer's descriptor-bound proof.
/// An observer error refuses the acknowledgement after sync; recovery retains
/// the durable WAL bytes rather than treating an unavailable index as current.
pub type PublicationObserverFactory =
    dyn Fn(&BindIdentity, &Path, broca_wal::WalLineageId) -> Arc<PublicationObserver> + Send + Sync;
pub type PublicationObserver = dyn Fn(broca_wal::file_wal::DurablePublicationRef<'_>) -> Result<(), &'static str>
    + Send
    + Sync;
pub type SessionGuard = dyn Fn(&BindIdentity) -> Result<(), String> + Send + Sync;

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

#[cfg(any(test, feature = "test-clock"))]
thread_local! {
    static ARCHIVE_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static ARCHIVE_LISTINGS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static ARCHIVE_STATS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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

#[cfg(unix)]
fn same_wal_file(left: &Path, right: &Path) -> Result<bool, WalError> {
    use std::os::unix::fs::MetadataExt;
    let left = broca_wal::fs_ops::metadata(left).map_err(WalError::Io)?;
    let right = broca_wal::fs_ops::metadata(right).map_err(WalError::Io)?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(windows)]
fn same_wal_file(left: &Path, right: &Path) -> Result<bool, WalError> {
    let left = broca_wal::fs_ops::File::open(left).map_err(WalError::Io)?;
    let right = broca_wal::fs_ops::File::open(right).map_err(WalError::Io)?;
    // Placement repair may remove an alias, so compare the complete handle identities,
    // not the lossy hashes used to invalidate read caches.
    Ok(same_native_file_identity(
        left.native_file_identity(),
        right.native_file_identity(),
    ))
}

#[cfg(not(any(unix, windows)))]
fn same_wal_file(_left: &Path, _right: &Path) -> Result<bool, WalError> {
    // Never guess whether paths are aliases when native file identity is unavailable.
    Ok(false)
}

impl FileWalStore {
    /// [`Self::owned_by_store`] for a session known only by its address.
    pub fn address_owned_by_store(&self, address: &str) -> Result<bool, WalError> {
        broca_wal::owned_by_store(self.state_root(), address).map_err(WalError::Io)
    }

    fn retry_live_flush(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.read_path(session)?)
            .map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        self.retry_live_identity(session)
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

    /// Where a session of `class` keeps its live WAL: the ephemeral, durable or ordinary folder.
    pub fn class_wal_path(
        &self,
        class: broca_wal::StorageClass,
        session: &BindIdentity,
    ) -> PathBuf {
        match class {
            broca_wal::StorageClass::Ephemeral => self.ephemeral_wal_path(session),
            broca_wal::StorageClass::Durable => self.durable_wal_path(session),
            broca_wal::StorageClass::Ordinary => self.wal_path(session),
        }
    }

    /// Only an owned open may finish a cut link/unlink placement. Discovery
    /// cannot choose a direction while another writer may still be relocating.
    fn finish_linked_placement(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
    ) -> Result<(), WalError> {
        let paths = [
            self.wal_path(session),
            self.ephemeral_wal_path(session),
            self.durable_wal_path(session),
        ];
        let mut live = Vec::new();
        for path in &paths {
            if broca_wal::wal_file_exists(path)? {
                live.push(path);
            }
        }
        if live.len() < 2 {
            return Ok(());
        }
        for path in &live[1..] {
            if !same_wal_file(live[0], path)? {
                return Err(WalError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "conflicting live WALs: {} and {}",
                        live[0].display(),
                        path.display()
                    ),
                )));
            }
        }
        let read = broca_wal::read_records_range(live[0], self.max_read_bytes, 0, None, None)?;
        read.check_run_compatible()?;
        let records = match read.records() {
            broca_wal::WalRangeRecords::Complete(records) => records,
            _ => {
                return Err(WalError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "linked WAL disappeared before placement repair",
                )))
            }
        };
        if records.iter().any(|frame| {
            !matches!(
                frame.record,
                WalRecord::WriterOpened { .. }
                    | WalRecord::WriterClosed { .. }
                    | WalRecord::VisibilityLate { .. }
            )
        }) {
            return Err(WalError::Io(io::Error::new(io::ErrorKind::InvalidData,
                "wal_relocation_incomplete: a started session cannot have a pending first-send placement")));
        }
        let destination = &paths[match self.placement(retention, session_class) {
            broca_wal::StorageClass::Ordinary => 0,
            broca_wal::StorageClass::Ephemeral => 1,
            broca_wal::StorageClass::Durable => 2,
        }];
        let parent = destination.parent().expect("store WAL paths have a parent");
        let incomplete_paths = live
            .iter()
            .map(|path| path.as_path().to_path_buf())
            .collect();
        let repair = || -> io::Result<()> {
            if !live.contains(&destination) {
                broca_wal::migration::durable_dir(parent.parent().unwrap_or(parent), parent)?;
                broca_wal::fs_ops::hard_link(live[0], destination)?;
            }
            broca_wal::fs_ops::sync_dir(parent)?;
            for source in live {
                if source == destination {
                    continue;
                }
                match broca_wal::fs_ops::remove_file(source) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                broca_wal::fs_ops::sync_dir(
                    source.parent().expect("store WAL paths have a parent"),
                )?;
            }
            Ok(())
        };
        repair().map_err(|error| WalError::RelocationIncomplete {
            paths: incomplete_paths,
            cause: Some(error.to_string()),
        })
    }

    fn flush_for_authorization_with_lookup(
        &self,
        session: &BindIdentity,
        read_identity: impl FnOnce(&Path) -> Result<Option<u64>, WalError>,
        lookup: &ArchiveLookup,
    ) -> Result<Option<u64>, WalError> {
        self.check_authorization_move(session)?;
        if let Some(member) = &lookup.member {
            // A sealed archive was fsynced before publication and its digest just verified.
            // Reads need no write handle or extraction to prove these immutable bytes.
            return Ok(Some(member_identity(member)));
        }
        // This is deliberately not FileWal::open: recovery-open may truncate a torn tail, while
        // a read-side proof is allowed to fsync only. Write access is required by Windows
        // FlushFileBuffers, and Rust's shared open mode permits this alongside the append writer.
        let path = self.read_path(session)?;
        let file = match std::fs::OpenOptions::new().write(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // A live WAL observed above may have moved into an archive before open. Re-discover
                // archives here; the expiry inventory predates that publication.
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
        // cannot be cached, not that another flush is needed.
        // Reopening by pathname could race retention deletion and flush a different lineage
        // or fail after the original WAL has moved to its deleting path.
        Ok(identity)
    }

    fn retry_live_identity(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        // Unlike the public identity helper, a retry must not turn NotFound into None.
        // Platforms without file identities still return None for an existing file.
        let path = self.read_path(session)?;
        std::fs::metadata(&path).map_err(WalError::Io)?;
        #[cfg(test)]
        AFTER_RETRY_LIVE_STAT.with(|hook| {
            if let Some(action) = hook.borrow_mut().take() {
                action();
            }
        });
        let identity = broca_wal::file_identity(&path)?;
        if identity.is_none() {
            // The identity helper also returns None when its open misses a disappearing
            // path. Only an existing identityless backend may return a successful None here.
            std::fs::metadata(&path).map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    WalError::Io(io::Error::new(
                        io::ErrorKind::NotFound,
                        "live WAL disappeared again during identity retry",
                    ))
                } else {
                    WalError::Io(error)
                }
            })?;
        }
        Ok(identity)
    }

    fn flush_for_authorization_with_identity(
        &self,
        session: &BindIdentity,
        read_identity: impl FnOnce(&Path) -> Result<Option<u64>, WalError>,
    ) -> Result<Option<u64>, WalError> {
        self.check_authorization_move(session)?;
        let lookup = self.archived_member(session)?;
        self.flush_for_authorization_with_lookup(session, read_identity, &lookup)
    }

    fn flush_for_read_with_identity(
        &self,
        session: &BindIdentity,
        read_identity: impl FnOnce(&Path) -> Result<Option<u64>, WalError>,
    ) -> Result<Option<u64>, WalError> {
        self.check_move(session, false)?;
        self.flush_for_authorization_with_identity(session, read_identity)
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

    fn check_authorization_move(&self, session: &BindIdentity) -> Result<(), WalError> {
        match self.check_move(session, false) {
            Err(WalError::SessionMoving { .. } | WalError::SessionMoved { .. }) => Ok(()),
            result => result,
        }
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
        if self.config.is_overhaul() {
            if let extraction::RestoreAuthority::OwnedTenure { lease, .. } = &authority {
                if lease.key() != &crate::lease::lease_key_for(session) {
                    return Err(WalError::Io(io::Error::new(io::ErrorKind::PermissionDenied,
                        "wal_relocation_incomplete: placement repair requires this session's owned tenure")));
                }
                self.finish_linked_placement(session, retention, session_class)?;
            }
        }
        let lookup = self.archived_member(session)?;
        let path = if let Some(member) = lookup.member {
            broca_wal::file_wal::check_member_run_compatible(&member.payload)?;
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
        } else {
            self.class_wal_path(self.placement(retention, session_class), session)
        };
        let identity = format!(
            "{}\u{1f}{}\u{1f}{}",
            session.project_root.display(),
            session.harness,
            session.session
        );
        let retained = path == self.ephemeral_wal_path(session);
        let wal = match self.recovery_hooks(session) {
            Some(hooks) => FileWal::open_with_manifest_and_recovery_hooks(
                path.clone(),
                self.manifest_path(),
                session_addr(session),
                identity,
                agent_session_ref(session),
                (self.config, self.feature_floor_path()),
                hooks,
            ),
            None => FileWal::open_with_manifest_and_recovery(
                path.clone(),
                self.manifest_path(),
                session_addr(session),
                identity,
                agent_session_ref(session),
                (self.config, self.feature_floor_path()),
            ),
        }?
        .with_recovery_cap(self.max_read_bytes)
        .with_feature_floor(self.feature_floor_path())
        .with_wal_config(self.config)?;
        let retained_policy = if retained {
            let records = broca_wal::access_counter::writer_open(&path, false, || wal.replay())?;
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
        let publication_observer = self
            .publication_observer
            .as_ref()
            .and_then(|factory| Some(factory(session, wal.log_path()?, wal.lineage_id()?)));
        Ok((
            Box::new(retention_changes::ObservedWal {
                inner: wal,
                address: Arc::from(session_addr(session)),
                changes: self.retention_changes.clone(),
                retained: retained.clone(),
                session: session.clone(),
                durable_observer: self.durable_observer.clone(),
                publication_observer,
            }),
            retained,
        ))
    }

    fn read_path(&self, session: &BindIdentity) -> Result<PathBuf, WalError> {
        self.read_address_path(&session_addr(session))
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

    fn recovery_hooks(
        &self,
        session: &BindIdentity,
    ) -> Option<Arc<dyn broca_wal::recovery::RecoveryHooks>> {
        if !self.config.is_overhaul() {
            return None;
        }
        let views = self.recovery_views.lock().unwrap().snapshot(session);
        let history = self.served_history.lock().unwrap().clone();
        (!views.is_empty() || history.is_some()).then(|| {
            Arc::new(ProjectionRecovery {
                history,
                session: session.clone(),
                views,
                max_bytes: self.max_read_bytes,
            }) as Arc<dyn broca_wal::recovery::RecoveryHooks>
        })
    }

    pub fn set_served_history(&self, history: Arc<dyn crate::ServedHistory>) {
        *self.served_history.lock().unwrap() = Some(history);
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

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The state root this store's WAL folder lives in.
    fn state_root(&self) -> &Path {
        self.root.parent().unwrap_or_else(|| Path::new(""))
    }

    /// How a reader of this store refuses `session` when the new session storage owns it: as
    /// [`Self::check_move`] refuses a session whose head exists, or `SessionMoving` under the
    /// move's id while a move is in flight, which a caller may retry. `None` when today's
    /// storage owns the session.
    pub fn store_owner_refusal(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<WalError>, WalError> {
        Ok(
            match broca_wal::store_ownership(self.state_root(), &session_addr(session))
                .map_err(WalError::Io)?
            {
                broca_wal::StoreOwnership::Today => None,
                broca_wal::StoreOwnership::Held => Some(session_storage_refusal()),
                broca_wal::StoreOwnership::Moving { move_id } => {
                    Some(WalError::SessionMoving { cut_id: move_id })
                }
            },
        )
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

    /// Resolve media retention from the session's actual live or archived WAL location.
    pub fn media_store_class(
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
            served_history: Mutex::new(None),
            recovery_views: Mutex::new(RecoveryViews::default()),
            root,
            config,
            refusal,
            backup_classes: vec!["head".into()],
            max_read_bytes: DEFAULT_MAX_READ_BYTES,
            session_index: Arc::default(),
            move_reads: AtomicU64::new(0),
            retention_changes: Arc::default(),
            durable_observer: None,
            publication_observer: None,
            session_guard: None,
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

    /// The class a new session's WAL is placed in ([`broca_wal::placement`] over this store's
    /// `backup_classes`).
    fn placement(
        &self,
        retention: Option<u64>,
        session_class: Option<&str>,
    ) -> broca_wal::StorageClass {
        broca_wal::placement(
            retention.is_some(),
            broca_wal::selected_for_backup(session_class, &self.backup_classes),
        )
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
            self.placement(None, session_class) == broca_wal::StorageClass::Durable
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

    /// Locate the live file without choosing between conflicting lineages. Files
    /// moved to `.deleting` and tombstone directories are never live WAL inputs.
    pub fn resolve_live_path(&self, session: &BindIdentity) -> Result<Option<PathBuf>, WalError> {
        self.resolve_live_address(&session_addr(session))
    }

    fn resolve_live_address(&self, address: &str) -> Result<Option<PathBuf>, WalError> {
        let mut live: Option<PathBuf> = None;
        let mut aliases = Vec::new();
        for root in [&self.root, &self.ephemeral_root(), &self.durable_root()] {
            let path = root.join(format!("{address}.wal"));
            if broca_wal::wal_file_exists(&path)? {
                if let Some(prior) = live {
                    if self.config.is_overhaul() && same_wal_file(&prior, &path)? {
                        if aliases.is_empty() {
                            aliases.push(prior.clone());
                        }
                        aliases.push(path);
                        live = Some(prior);
                        continue;
                    }
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
        if !aliases.is_empty() {
            return Err(WalError::RelocationIncomplete {
                paths: aliases,
                cause: None,
            });
        }
        Ok(live)
    }

    fn read_address_path(&self, address: &str) -> Result<PathBuf, WalError> {
        Ok(self
            .resolve_live_address(address)?
            .unwrap_or_else(|| self.root.join(format!("{address}.wal"))))
    }

    pub fn max_read_bytes(&self) -> u64 {
        self.max_read_bytes
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
        let paths = self.archive_containers()?;
        self.archived_member_in_paths(address, paths.as_deref())
    }

    fn archived_member_in_paths(
        &self,
        address: &str,
        paths: Option<&[PathBuf]>,
    ) -> Result<ArchiveLookup, WalError> {
        let Some(paths) = paths else {
            return Ok(ArchiveLookup::default());
        };
        #[cfg(any(test, feature = "test-clock"))]
        ARCHIVE_LOOKUPS.with(|count| count.set(count.get() + 1));
        let (durable_paths, ordinary_paths): (Vec<_>, Vec<_>) =
            paths.iter().cloned().partition(|path| {
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
        #[cfg(any(test, feature = "test-clock"))]
        ARCHIVE_LISTINGS.with(|count| count.set(count.get() + 1));
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

    fn read_address_range_for_authorization(
        &self,
        address: &str,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        let lookup = self.archived_member_at_address(address)?;
        self.read_address_range_with_lookup(address, after_seq, through_seq, anchor, &lookup)
    }

    fn read_address_range_with_lookup(
        &self,
        address: &str,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&WalByteAnchor>,
        lookup: &ArchiveLookup,
    ) -> Result<WalRangeRead, WalError> {
        #[cfg(any(test, feature = "test-clock"))]
        self.range_reads.fetch_add(1, Ordering::Relaxed);
        if let Some(member) = &lookup.member {
            return member_range(member, after_seq, through_seq);
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
            // A live WAL may move into an archive before open, or a tree with no archive
            // may gain one. Retry discovery only after a failed live read, not per step.
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

/// The refusal today's storage gives a session kept in the new session storage: it has no WAL
/// here, and a build that serves the new storage reads it from there. The feature list names the
/// new storage, so a caller can tell this refusal from a WAL frame this build can't read.
pub fn session_storage_refusal() -> WalError {
    WalError::NewerWriter {
        offset: 0,
        ver: 0,
        missing: vec![broca_wal::SESSION_STORAGE_FEATURE.to_owned()],
    }
}

/// Whether the new session storage's head file at `path` exists: one `symlink_metadata`, taken
/// directly rather than through the WAL's file operations, so checking a session's heads adds
/// nothing to the I/O today's storage performs through them. Only a head found absent is absent.
fn head_present(path: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Methods the `WalStore` implementation below delegates to.
impl FileWalStore {
    pub fn tombstone_path(&self, session: &BindIdentity, lineage: &str) -> PathBuf {
        self.ephemeral_root()
            .join(format!("{}.expired", session_addr(session)))
            .join(lineage)
    }

    pub fn check_move(&self, session: &BindIdentity, write: bool) -> Result<(), WalError> {
        if let Some(guard) = &self.session_guard {
            guard(session).map_err(|error| WalError::Io(io::Error::other(error)))?;
        }
        self.check_session_storage(session)?;
        // A cut file may name the session by its folder-based v1 reference (recorded before
        // the v2 switch) or by its v2 reference. Check both in one pass, so a pending release
        // under either reference outranks a moved tombstone for writes.
        let legacy = legacy_session_ref(
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        );
        let current = agent_session_ref(session);
        let wal = self.wal_path(session);
        if self.config.is_overhaul() && !write {
            // A reader must not expose a partly installed activation. Recovery
            // reads WAL descriptors directly and does not pass this serving gate.
            for reference in [&legacy, &current] {
                if let Some(cut_id) = broca_wal::migration::cut_fence(
                    wal.parent().unwrap_or(Path::new(".")),
                    reference,
                )
                .map_err(WalError::Io)?
                {
                    return Err(WalError::SessionMoving { cut_id });
                }
            }
        }
        broca_wal::migration::guard_refs(&wal, &[&legacy, &current], write)
    }

    /// Refuses a session kept in the new session storage: it has no WAL here, and this build
    /// doesn't serve it. Every read and write of a session checks this first, so no path
    /// answers for it from an absent WAL or creates one, which would fork the session in two.
    /// A head that can't be checked refuses too, rather than reading as absent.
    fn check_session_storage(&self, session: &BindIdentity) -> Result<(), WalError> {
        match broca_wal::session_storage_class_by(
            self.state_root(),
            &session_addr(session),
            head_present,
        ) {
            Ok(Some(_)) => Err(session_storage_refusal()),
            Ok(None) => Ok(()),
            Err(error) => Err(WalError::Io(error)),
        }
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
        self.lineage_exists_with_lookup(session, meta, &lookup)
    }

    fn lineage_exists_with_lookup(
        &self,
        session: &BindIdentity,
        meta: Option<&crate::AdmissionStoreSnapshot>,
        lookup: &ArchiveLookup,
    ) -> Result<bool, WalError> {
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
}

impl WalStore for FileWalStore {
    fn served_history(&self) -> Option<Arc<dyn crate::ServedHistory>> {
        self.served_history.lock().unwrap().clone()
    }

    fn bind_recovery_projection(
        &self,
        session: Option<&BindIdentity>,
        store: Arc<dyn crate::SessionStore>,
    ) {
        if self.config.is_overhaul() {
            self.recovery_views.lock().unwrap().bind(session, store);
        }
    }
    fn recover_existing(&self, session: &BindIdentity) -> Result<(), WalError> {
        if !self.config.is_overhaul() {
            return Ok(());
        }
        self.check_move(session, true)?;
        if let Some(path) = self.resolve_live_path(session)? {
            drop(match self.recovery_hooks(session) {
                Some(hooks) => FileWal::open_with_manifest_and_recovery_hooks(
                    path,
                    self.manifest_path(),
                    session_addr(session),
                    String::new(),
                    agent_session_ref(session),
                    (self.config, self.feature_floor_path()),
                    hooks,
                ),
                None => FileWal::open_with_manifest_and_recovery(
                    path,
                    self.manifest_path(),
                    session_addr(session),
                    String::new(),
                    agent_session_ref(session),
                    (self.config, self.feature_floor_path()),
                ),
            }?);
        }
        Ok(())
    }
    fn read_media_seed_suffix(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: u64,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.check_move(session, false)?;
        if let Some(anchor) = anchor {
            if self.archived_member(session)?.member.is_none() {
                let limit = anchor
                    .byte_end
                    .checked_add(self.max_read_bytes)
                    .ok_or_else(|| {
                        WalError::Io(io::Error::other("media suffix read budget overflow"))
                    })?;
                if let Some(range) = broca_wal::read_records_range_from_valid_anchor(
                    self.read_path(session)?,
                    limit,
                    after_seq,
                    Some(through_seq),
                    anchor,
                )? {
                    return Ok(range);
                }
            }
        }
        // Archive members keep their digest verification; a moved/folded live file
        // must prove the requested suffix again instead of trusting an old inode offset.
        self.read_records_range(session, after_seq, Some(through_seq), anchor)
    }
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
    fn store_owner_refusal(&self, session: &BindIdentity) -> Result<Option<WalError>, WalError> {
        FileWalStore::store_owner_refusal(self, session)
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
        self.wal_present_checked(session).ok().flatten()
    }

    fn wal_present_checked(&self, session: &BindIdentity) -> Result<Option<bool>, WalError> {
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
        // An IO failure is not absence. Preserve it for checked callers; the
        // legacy Option adapter above still maps that failure to unknown, not false.
        self.archived_member(session)
            .map(|lookup| Some(lookup.member.is_some() || lookup.live_present))
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
    static AFTER_RETRY_LIVE_STAT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
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
    reopen_reserve: u64,
    retained: bool,
    session_max_bytes: u64,
    max_bytes: u64,
    used_bytes: u64,
    overhead: u64,
    reserve: u64,
    refused: bool,
}

impl AppendBudget {
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

    fn scope(&self) -> retained_size::SizeScope {
        if self.retained {
            retained_size::SizeScope::RetainedSession
        } else {
            retained_size::SizeScope::Session
        }
    }
}

/// A failure reported by WalHandle::sync while committing queued records to disk.
/// I/O failures are fatal. A byte-budget refusal leaves accepted
/// records writable so the run boundary can append and commit an Error terminal.
#[derive(Debug, Clone)]
pub enum WalWriteError {
    Failed(String),
    RelocationIncomplete { reason: String },
    ProofPathUnavailable,
    RetainedSessionTooLarge { max_bytes: u64 },
    SessionTooLarge { max_bytes: u64 },
}

impl std::fmt::Display for WalWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(detail) => write!(f, "wal write failure: {detail}"),
            Self::RelocationIncomplete { reason } => f.write_str(reason),
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

impl WalWriteError {
    fn detail(&self) -> String {
        match self {
            Self::Failed(detail) => detail.clone(),
            Self::RelocationIncomplete { reason } => reason.clone(),
            Self::ProofPathUnavailable => self.to_string(),
            Self::SessionTooLarge { .. } => self.to_string(),
            Self::RetainedSessionTooLarge { .. } => self.to_string(),
        }
    }

    fn from_wal(error: WalError) -> Self {
        match error {
            error @ WalError::RelocationIncomplete { .. } => Self::RelocationIncomplete {
                reason: error.to_string(),
            },
            other => Self::Failed(other.to_string()),
        }
    }
}
impl From<String> for WalWriteError {
    fn from(detail: String) -> Self {
        Self::Failed(detail)
    }
}
impl From<&str> for WalWriteError {
    fn from(detail: &str) -> Self {
        Self::Failed(detail.into())
    }
}

enum WalCommand {
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
    fn now_ms(&self) -> u64 {
        (self.0)()
    }

    pub(crate) fn wall() -> Self {
        Self(Arc::new(broca_wal::framing::appended_ms_now))
    }

    #[cfg(any(test, feature = "test-clock"))]
    pub(crate) fn from_fn(clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self(Arc::new(clock))
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
    step_events: Option<StepEventObserver>,
    synced_head: Option<Arc<AtomicU64>>,
    publications: Vec<tokio::sync::watch::Sender<Option<broca_wal::file_wal::DurablePublication>>>,
}

/// Terminal attempt evidence copied after durability for a best-effort live hint. The
/// bounded receiver is independent of transcript subscribers and the metadata tap.
#[derive(Clone, Debug)]
pub struct DurableAttempt {
    pub seq: u64,
    pub lineage: Option<broca_wal::WalLineageId>,
    pub writer_id: Option<[u8; 16]>,
    pub interrupted_at_ms: Option<u64>,
    pub attempt_id: broca_wal::record::AttemptId,
    pub reservation: Box<broca_wal::record::AttemptReservation>,
    pub envelope: Box<broca_wal::record::AttemptEnvelope>,
    /// The caller's frozen RunConfig decision, never the account allocator's decision id.
    pub route_decision_id: Option<String>,
    /// Content intervals derived from recorded attempt stamps, not completion latency.
    pub sent_to_last_content_ms: Option<u64>,
    pub first_to_last_content_ms: Option<u64>,
    pub cache: Option<StepEventCache>,
}

/// Live-only cache evidence. It is never serialized into the session log.
#[derive(Clone, Debug)]
pub struct StepEventCache {
    pub frozen_render_config: serde_json::Value,
    /// None means unavailable; an omitted setting at admission is explicitly Off.
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    pub stop_reason: Option<String>,
}

#[derive(Clone)]
struct StepEventObserver {
    tx: tokio::sync::mpsc::Sender<DurableAttempt>,
    dropped: Arc<AtomicU64>,
    open: std::collections::BTreeMap<broca_wal::record::AttemptId, DurableAttempt>,
    // A cache-maintenance request can finish after the next model run starts; keep decision ids
    // until this WAL writer closes.
    runs: std::collections::BTreeMap<String, Option<String>>,
    caches: std::collections::BTreeMap<String, StepEventCache>,
    current_run: Option<String>,
}

impl StepEventObserver {
    fn evidence(attempt: &DurableAttempt, synced_seq: Option<u64>) {
        if let (Some(synced_seq), Some(lineage), Some(writer)) =
            (synced_seq, attempt.lineage, attempt.writer_id)
        {
            tracing::debug!(target: "step-events.evidence",
                storage = "wal", session_ref = %attempt.reservation.session.session_ref,
                log_incarnation = %format!("{lineage}.{}", writer.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
                step_seq = attempt.seq, synced_seq, attempt_id = %attempt.attempt_id.as_str(),
                "step event durable handoff");
        }
    }

    fn route_decision(&self, run_id: &broca_protocol::RunId) -> Option<String> {
        self.runs
            .get(&run_id.0)
            .and_then(|decision| decision.clone())
    }

    fn note_cache(&mut self, run_id: &broca_protocol::RunId, mut cache: StepEventCache) {
        if let Some(prior) = self.caches.get(&run_id.0) {
            cache.keep_warm = cache.keep_warm.or(prior.keep_warm.clone());
            cache.stop_reason.clone_from(&prior.stop_reason);
        }
        self.caches.insert(run_id.0.clone(), cache);
    }

    fn note_run(&mut self, run_id: &broca_protocol::RunId, route_decision_id: Option<String>) {
        for attempt in self
            .open
            .values_mut()
            .filter(|attempt| attempt.reservation.run_id == *run_id)
        {
            attempt.route_decision_id.clone_from(&route_decision_id);
        }
        self.runs.insert(run_id.0.clone(), route_decision_id);
    }

    fn observe(
        &mut self,
        seq: u64,
        lineage: Option<broca_wal::WalLineageId>,
        writer_id: Option<[u8; 16]>,
        record: &WalRecord,
        synced_seq: Option<u64>,
    ) {
        match record {
            WalRecord::RunStarted {
                run_id,
                config,
                keep_warm,
                ..
            } => {
                self.current_run = Some(run_id.0.clone());
                self.note_run(run_id, config.route_decision_id.clone());
                self.note_cache(
                    run_id,
                    StepEventCache {
                        frozen_render_config: config.frozen_render_config.clone(),
                        keep_warm: Some(keep_warm.clone().unwrap_or_default()),
                        stop_reason: None,
                    },
                );
            }
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => {
                if let Some(cache) = self
                    .current_run
                    .as_ref()
                    .and_then(|id| self.caches.get_mut(id))
                {
                    cache.frozen_render_config.clone_from(frozen_render_config);
                }
            }
            WalRecord::AttemptReserved {
                attempt_id,
                reservation,
            } => {
                self.open.insert(
                    attempt_id.clone(),
                    DurableAttempt {
                        seq,
                        lineage,
                        writer_id,
                        interrupted_at_ms: None,
                        attempt_id: attempt_id.clone(),
                        reservation: reservation.clone(),
                        envelope: Default::default(),
                        route_decision_id: self.route_decision(&reservation.run_id),
                        sent_to_last_content_ms: None,
                        first_to_last_content_ms: None,
                        cache: self.caches.get(&reservation.run_id.0).cloned(),
                    },
                );
            }
            WalRecord::ModelAttemptFinished {
                attempt_id: Some(id),
                reservation: Some(reservation),
                envelope: Some(envelope),
                ..
            }
            | WalRecord::CacheWarmed {
                attempt_id: Some(id),
                reservation: Some(reservation),
                envelope: Some(envelope),
                ..
            }
            | WalRecord::AttemptFinished {
                attempt_id: id,
                reservation,
                envelope,
            } => {
                let opened = self.open.remove(id);
                // Reserve before copying evidence: a full queue costs no record clone.
                match self.tx.try_reserve() {
                    Ok(slot) => {
                        let (sent_to_last_content_ms, first_to_last_content_ms) = match record {
                            WalRecord::ModelAttemptFinished {
                                sent_at_ms,
                                content_progress,
                                ..
                            } => crate::replay::content_timing_intervals(
                                *sent_at_ms,
                                content_progress.first_content_at_ms,
                                content_progress.last_content_at_ms,
                            ),
                            _ => (None, None),
                        };
                        let attempt = DurableAttempt {
                            seq,
                            lineage,
                            writer_id,
                            interrupted_at_ms: None,
                            attempt_id: id.clone(),
                            reservation: reservation.clone(),
                            envelope: envelope.clone(),
                            route_decision_id: opened
                                .and_then(|attempt| attempt.route_decision_id)
                                .or_else(|| self.route_decision(&reservation.run_id)),
                            sent_to_last_content_ms,
                            first_to_last_content_ms,
                            cache: self.caches.get(&reservation.run_id.0).cloned(),
                        };
                        Self::evidence(&attempt, synced_seq);
                        slot.send(attempt)
                    }
                    Err(_) => {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            WalRecord::AttemptReleased { attempt_id, .. } => {
                self.open.remove(attempt_id);
            }
            WalRecord::WriterClosed { .. } => {
                let finished_at_ms = StampClock::wall().now_ms();
                for (_, mut attempt) in std::mem::take(&mut self.open) {
                    attempt.interrupted_at_ms = Some(finished_at_ms);
                    attempt.cache = self.caches.get(&attempt.reservation.run_id.0).cloned();
                    Self::evidence(&attempt, synced_seq);
                    if self.tx.try_send(attempt).is_err() {
                        self.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            // Opening must not replay the log just for best-effort hints. Retain only
            // this writer's reservations; durable readers classify earlier writers'
            // open reservations as possibly billed.
            WalRecord::WriterOpened { .. } => {
                self.open.clear();
                self.runs.clear();
                self.caches.clear();
                self.current_run = None;
            }
            // Corrections are not another attempt and must not create another hint.
            _ => {}
        }
    }
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
pub struct ObservedRecord(
    Arc<(
        broca_wal::FramedRecord,
        Option<broca_wal::WalLineageId>,
        Option<broca_wal::framing::CursorFrameIdentity>,
    )>,
);

impl ObservedRecord {
    pub fn new(framed: broca_wal::FramedRecord) -> Self {
        Self(Arc::new((framed, None, None)))
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
    /// Install a lossy live-hint queue, never a billing outbox. Reserving a slot without
    /// waiting keeps a slow or disconnected publisher out of the WAL durability path.
    pub fn with_step_events(
        mut self,
        tx: tokio::sync::mpsc::Sender<DurableAttempt>,
        dropped: Arc<AtomicU64>,
    ) -> Self {
        self.step_events = Some(StepEventObserver {
            tx,
            dropped,
            open: Default::default(),
            runs: Default::default(),
            caches: Default::default(),
            current_run: None,
        });
        self
    }

    fn observe_publication(&self, wal: &dyn Wal) {
        if self.publications.is_empty() {
            return;
        }
        if let Some(publication) = wal.durable_publication() {
            for tx in &self.publications {
                tx.send_replace(Some(publication.clone()));
            }
        }
    }

    fn observe_with_identity(
        &self,
        seq: u64,
        fence: u64,
        record: &WalRecord,
        lineage: Option<broca_wal::WalLineageId>,
        identity: Option<broca_wal::framing::CursorFrameIdentity>,
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
            identity.filter(|identity| identity.seq == seq),
        ))));
    }

    fn observe_tap(
        &mut self,
        seq: u64,
        fence: u64,
        lineage: Option<broca_wal::WalLineageId>,
        writer_id: Option<[u8; 16]>,
        record: &WalRecord,
    ) {
        if let Some(events) = &mut self.step_events {
            events.observe(
                seq,
                lineage,
                writer_id,
                record,
                self.synced_head
                    .as_ref()
                    .map(|head| head.load(Ordering::Acquire)),
            );
        }
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

/// A writer's WAL and the session lease beside it, held together until the writer thread
/// takes them. Dropping it closes the WAL before releasing the lease, so no early exit
/// between a writer open and its thread start leaves a writable WAL without its lease.
struct WriterOwner {
    wal: Option<Box<dyn Wal + Send>>,
    lease: Option<Box<dyn LeaseHandle>>,
}

impl WriterOwner {
    fn into_parts(mut self) -> (Box<dyn Wal + Send>, Option<Box<dyn LeaseHandle>>) {
        let wal = self
            .wal
            .take()
            .expect("writer owner holds its WAL until handoff");
        (wal, self.lease.take())
    }

    fn wal(&self) -> &(dyn Wal + Send) {
        self.wal
            .as_deref()
            .expect("writer owner holds its WAL until handoff")
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
    ride_ledger: Mutex<crate::delivery_order::RideLedger>,
    rides_writer_enabled: AtomicBool,
    /// Cut boundaries namespace later calls without changing keys of retained Prepared frames.
    physical_key_generations: Vec<(u64, String)>,
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
    poisoned: Arc<Mutex<Option<WalWriteError>>>,
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
    /// run loop attaches them to the messages it adds to its in-memory prompt. Retained only
    /// while a run that renders tags asked for them (`None` otherwise), so a run that does not
    /// render tags keeps nothing.
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

    fn shutdown_blocking_keeping_lease(
        &mut self,
    ) -> (Result<(), WalWriteError>, Option<Box<dyn LeaseHandle>>) {
        let Some(join) = self.join.take() else {
            return (
                current_write_error(&self.poisoned).map_or(Ok(()), Err),
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
        if let Some(error) = current_write_error(&self.poisoned) {
            return (Err(error), lease);
        }
        let result = result.recv().unwrap_or_else(|_| {
            let error = "WAL writer dropped the shutdown acknowledgement".to_string();
            set_poison(&self.poisoned, error.clone());
            Err(WalWriteError::Failed(error))
        });
        (result, lease)
    }

    fn enqueue_record_with(
        &self,
        mut record: WalRecord,
        reserved_terminal: bool,
        assigned: &mut Option<Vec<MessageTags>>,
    ) -> RecordOutcome {
        // The enqueue lock also allocates this sequence. Persist the generated
        // ordinary key so readers replaying only a WAL suffix need not reconstruct
        // which repair cut preceded the record.
        if !self.physical_key_generations.is_empty() {
            if let WalRecord::ToolDispatchIntent { call_key, .. } = &mut record {
                if call_key.is_none() {
                    *call_key = self.call_key(self.next_seq.load(Ordering::SeqCst));
                }
            }
        }
        {
            let mut ledger = self.ride_ledger.lock().unwrap();
            let lineage = self.lineage_id().map(|id| id.to_string());
            if self.rides_writer_enabled.load(Ordering::SeqCst) {
                let envelope = ledger.envelope(&record, lineage.as_deref());
                if let WalRecord::SendDelivered { ride, .. } = &mut record {
                    if ride.is_none() {
                        *ride = envelope;
                    }
                }
            }
            ledger.observe(&record, self.next_seq.load(Ordering::SeqCst));
        }
        if let Some(error @ WalWriteError::RelocationIncomplete { .. }) =
            current_write_error(&self.poisoned)
        {
            return RecordOutcome::Refused(error);
        }
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
                    .saturating_add(budget.reopen_reserve)
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

    fn spawn_proven_with_clock(
        wal: Box<dyn Wal + Send>,
        last_seq: u64,
        fence: u64,
        mut observer: Option<RecordObserver>,
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
        let config = wal.wal_config();
        let v4 = config.is_overhaul();
        if v4 && context.is_none() {
            return Err(WalWriteError::Failed(
                "writer-id/v1: overhaul spawn requires a store marker root and session address"
                    .into(),
            ));
        }
        if v4 && wal.log_path().is_none() {
            return Err(WalWriteError::ProofPathUnavailable);
        }
        // Lifecycle records do not start a conversation. A warmed writer-only log still needs
        // the same first-send placement, retention and runner-tag state as an empty log.
        let recovered_frames = if v4 {
            broca_wal::access_counter::writer_open(
                wal.log_path().unwrap_or(std::path::Path::new("")),
                false,
                || wal.replay(),
            )
            .map_err(|error| WalWriteError::Failed(error.to_string()))?
        } else {
            Vec::new()
        };
        let physical_key_generations = recovered_frames
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::RecoveryCut { recovery } => Some((frame.seq, recovery.cut_id.clone())),
                _ => None,
            })
            .collect();
        let unstarted = last_seq == 0
            || (v4
                && recovered_frames.iter().all(|frame| {
                    matches!(
                        frame.record,
                        WalRecord::WriterOpened { .. }
                            | WalRecord::WriterClosed { .. }
                            | WalRecord::VisibilityLate { .. }
                    )
                }));
        let mut ride_ledger = crate::delivery_order::RideLedger::default();
        for frame in &recovered_frames {
            ride_ledger.observe(&frame.record, frame.seq);
        }
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
        if let Some(observer) = &mut observer {
            observer.synced_head = Some(Arc::clone(&durable_head));
        }
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
                    reopen_reserve: retained_size::reopen_reserve_bytes(config),
                    retained: budget_scope == retained_size::SizeScope::RetainedSession,
                    session_max_bytes: recovery_cap.unwrap_or(max_bytes),
                    max_bytes,
                    used_bytes,
                    overhead,
                    reserve: retained_size::terminal_reserve_bytes(overhead, config),
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
                publisher: None,
                sequence: Some(writer_sequence),
                deferred: Default::default(),
                fence,
                clock: clock.clone(),
                observed: observer.is_some(),
            };
            let writer = if let Some(context) = context {
                let address = context.address.clone();
                match context.open(&mut **wal, fence, || clock.now_ms()) {
                    Ok((writer, seq, record, synced)) => {
                        let mut pending =
                            vec![(seq, Box::new(record), synced.last_cursor_frame_identity())];
                        let opened = broca_wal::head_proof::Publisher::from_synced(synced)
                            .map_err(WalWriteError::from_wal)
                            .and_then(|publisher| {
                                publication.publisher = Some(publisher);
                                publication.publish_synced(&mut **wal, &rx, &mut pending)
                            });
                        if let Err(error) = opened {
                            let _ = ready_tx.send(Err(error));
                            let (wal, lease) = owner.into_parts();
                            drop(wal);
                            return lease;
                        }
                        writer_durable_head.store(wal.last_seq(), Ordering::Release);
                        *writer_lineage_id.lock().unwrap() = wal.lineage_id();
                        if let Some(obs) = &mut observer {
                            for (seq, record, identity) in pending {
                                obs.observe_with_identity(
                                    seq,
                                    fence,
                                    &record,
                                    wal.lineage_id(),
                                    identity,
                                );
                                obs.observe_tap(
                                    seq,
                                    fence,
                                    wal.lineage_id(),
                                    wal.writer_id(),
                                    &record,
                                );
                            }
                        }
                        Some(writer)
                    }
                    Err(error) => {
                        let error = match error {
                            error @ WalError::RelocationIncomplete { .. } => {
                                WalWriteError::from_wal(error)
                            }
                            other => WalWriteError::Failed(format!(
                                "writer-id/v1: cannot open writer for {address}: {other}"
                            )),
                        };
                        let _ = ready_tx.send(Err(error));
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
            .spawn(broca_wal::access_counter::inherit(body))
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
            ride_ledger: Mutex::new(ride_ledger),
            rides_writer_enabled: AtomicBool::new(crate::delivery_order::RIDES_V2_WRITER_ENABLED),
            physical_key_generations,
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

    /// Barrier: fsync everything appended so far, await durability. Returns the last
    /// FSYNCED WAL seq — the durable head a control-stream cursor may advance to (NOT
    /// `last_seq`, which counts coalesced appends past the fsync barrier). Returns the
    /// FATAL write error if any append/fsync failed.
    pub async fn sync(&self) -> Result<u64, WalWriteError> {
        // Queue the barrier after every already-reserved append. This preserves the claim
        // that a successful ack covers everything callers had reserved before sync began.
        let (ack_rx, send_failed) = {
            let _guard = self.lock_enqueue();
            if let Some(error) = current_write_error(&self.poisoned) {
                return Err(error);
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

    pub(crate) fn call_key(&self, seq: u64) -> Option<String> {
        let lineage = self.lineage_id()?;
        let generation = self
            .physical_key_generations
            .iter()
            .rev()
            .find(|(cut_seq, _)| *cut_seq < seq)
            .map(|(_, id)| id.as_str());
        Some(broca_wal::recovery::physical_call_key(
            &lineage.to_string(),
            seq,
            generation,
        ))
    }

    /// The boot-decided format of this writer, fixed for its lifetime.
    pub fn is_overhaul(&self) -> bool {
        self.v4
    }

    pub fn media_capture(&self) -> Option<crate::tool_media::ToolMediaCapture> {
        self.media_capture.clone()
    }

    pub fn check_move(&self) -> Result<(), WalError> {
        match &self.move_check {
            Some(check) => check(),
            None => Ok(()),
        }
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
                identity: session.clone(),
                cause: None,
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
        Self::spawn_in_store_with_fs_and_clock_for_test(
            wal,
            store,
            session,
            fence,
            lease,
            fs,
            broca_wal::framing::appended_ms_now,
        )
    }

    /// Drive the real writer with deterministic stamp time and filesystem stalls.
    #[cfg(feature = "test-clock")]
    pub fn spawn_in_store_with_fs_and_clock_for_test<W: WalStore>(
        wal: Box<dyn Wal + Send>,
        store: &W,
        session: &BindIdentity,
        fence: u64,
        lease: Option<Box<dyn LeaseHandle>>,
        fs: Arc<dyn broca_wal::fs_ops::FsOps>,
        clock: impl Fn() -> u64 + Send + Sync + 'static,
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
        if !wal.wal_config().is_overhaul() {
            return Self::spawn_in_store(wal, store, session, fence, lease);
        }
        let fence = lease.as_ref().map_or(fence, |lease| lease.epoch());
        let (wal, seq) = Self::prove_recovery(wal)?;
        let context = store
            .writer_marker_root()
            .map(|root| open_writers::WriterContext {
                root,
                address: session_addr(session),
                identity: session.clone(),
                cause: None,
            });
        Self::spawn_proven_with_clock(
            wal,
            seq,
            fence,
            None,
            lease,
            (StampClock::from_fn(clock), context),
        )
    }

    fn prove_recovery(
        wal: Box<dyn Wal + Send>,
    ) -> Result<(Box<dyn Wal + Send>, u64), WalWriteError> {
        // FileWal::open has already recovered any torn tail under the caller's lease. Prove the
        // remaining complete prefix before publishing its sequence as durable: flushed bytes can
        // be readable even when a prior fsync failed. This is the one recovery barrier per spawn.
        wal.sync().map_err(WalWriteError::from_wal)?;
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

    /// The first run's policy, if this handle has proved it. Unknown is not forever.
    pub fn is_retention_lineage(&self) -> Option<bool> {
        match *self.lineage_retention.lock().unwrap() {
            Some(crate::LineageRetention::Retained { .. }) => Some(true),
            Some(crate::LineageRetention::Forever) => Some(false),
            _ => None,
        }
    }

    /// Queue a record and return explicit acceptance or refusal. Accepted sequences are
    /// not durable until sync succeeds. Callers that discard the result must sync before
    /// making a durability claim; refused appends have no sequence to publish.
    pub fn record(&self, record: WalRecord) -> RecordOutcome {
        let _guard = self.lock_enqueue();
        self.enqueue_record(record, false)
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

    fn enqueue_record(&self, record: WalRecord, reserved_terminal: bool) -> RecordOutcome {
        self.enqueue_record_with(record, reserved_terminal, &mut None)
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
type PendingObservedRecord = (
    u64,
    Box<WalRecord>,
    Option<broca_wal::framing::CursorFrameIdentity>,
);

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
        pending: &mut Vec<PendingObservedRecord>,
    ) -> Result<(), WalWriteError> {
        let Some(publisher) = &mut self.publisher else {
            return wal.sync().map_err(WalWriteError::from_wal);
        };
        publisher.sync(wal).map_err(WalWriteError::from_wal)?;
        self.publish_synced(wal, rx, pending)
    }

    fn publish_synced(
        &mut self,
        wal: &mut dyn Wal,
        rx: &std_mpsc::Receiver<WalCommand>,
        pending: &mut Vec<PendingObservedRecord>,
    ) -> Result<(), WalWriteError> {
        let publisher = self
            .publisher
            .as_mut()
            .expect("only overhaul writers publish proofs");
        loop {
            let (through_seq, oldest_appended_ms) = match publisher
                .publish(|| self.clock.now_ms())
                .map_err(|error| error.to_string())?
            {
                broca_wal::head_proof::PublicationStatus::Published => return Ok(()),
                broca_wal::head_proof::PublicationStatus::Late {
                    through_seq,
                    oldest_appended_ms,
                } => (through_seq, oldest_appended_ms),
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
                            pending.push((seq, record, wal.last_cursor_frame_identity()));
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
                pending.push((seq, Box::new(record), wal.last_cursor_frame_identity()));
            }
            // A second slow sync can age the marker too. Repeat, never publish
            // the late batch on the strength of a sync alone.
            crate::durability_counters::record_fsync(
                crate::durability_counters::FsyncCause::Barrier,
                self.fence,
            );
            publisher.sync(wal).map_err(WalWriteError::from_wal)?;
        }
    }
}

fn append_command(
    wal: &mut dyn Wal,
    fence: u64,
    clock: &StampClock,
    command: WalCommand,
) -> Result<(u64, Box<WalRecord>), WalWriteError> {
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
        Ok(assigned) => Err(WalWriteError::Failed(format!(
            "wal seq drift: handle assigned {seq}, wal assigned {assigned}"
        ))),
        Err(error) => Err(WalWriteError::from_wal(error)),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn legacy_read_move_check_never_consults_pending_cut_files() {
        let root = broca_test_support::scratch_root()
            .join(format!("legacy-read-cut-lookup-{}", std::process::id()));
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let wal = super::FileWalStore::new(root.join("wal"));
        let session = broca_protocol::BindIdentity::new("/project", "runner", "legacy-read");
        std::fs::create_dir_all(root.join("agent-data/cuts")).unwrap();
        std::fs::write(root.join("agent-data/cuts/unreadable.json"), b"not JSON").unwrap();
        assert!(
            wal.check_move(&session, false).is_ok(),
            "legacy reads gained a cut-state lookup"
        );
        assert!(
            wal.check_move(&session, true).is_err(),
            "the unreadable-cut control must reject a lookup"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A session with a head in any of the new session storage's roots is refused by name for
    /// reads and writes, so no path serves it from an absent WAL or creates one beside it. Other
    /// sessions at the same state root are untouched. A head path that can't be checked refuses
    /// too, instead of reading as no head.
    #[test]
    fn a_session_on_the_new_storage_is_refused_for_reads_and_writes() {
        let root = broca_test_support::scratch_root()
            .join(format!("session-storage-guard-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let wal = super::FileWalStore::new(root.join("wal"));
        let session = broca_protocol::BindIdentity::new("/project", "runner", "stored");
        let other = broca_protocol::BindIdentity::new("/project", "runner", "other");
        wal.check_move(&session, true).unwrap();
        for class in broca_wal::StorageClass::ALL {
            let head =
                broca_wal::session_storage_head(&root, class, &super::session_addr(&session));
            std::fs::create_dir_all(head.parent().unwrap()).unwrap();
            std::fs::write(&head, b"head").unwrap();
            for write in [false, true] {
                match wal.check_move(&session, write) {
                    Err(error @ broca_wal::WalError::NewerWriter { .. }) => {
                        let broca_wal::WalError::NewerWriter { missing, .. } = &error else {
                            unreachable!()
                        };
                        assert_eq!(missing, &[broca_wal::SESSION_STORAGE_FEATURE]);
                        assert!(
                            error.to_string().contains("newer session storage"),
                            "{error}"
                        );
                    }
                    other => panic!(
                        "{class} write={write}: expected the session-storage refusal: {other:?}"
                    ),
                }
            }
            wal.check_move(&other, true).unwrap();
            std::fs::remove_file(&head).unwrap();
        }
        // A file where the head's folder belongs makes the head uncheckable on unix, which
        // reports it as `NotADirectory`; Windows reports the same path as not found.
        #[cfg(unix)]
        {
            let blocked = broca_wal::session_storage_head(
                &root,
                broca_wal::StorageClass::Durable,
                &super::session_addr(&other),
            );
            std::fs::create_dir_all(blocked.parent().unwrap().parent().unwrap()).unwrap();
            std::fs::write(blocked.parent().unwrap(), b"not a folder").unwrap();
            match wal.check_move(&other, false) {
                Err(broca_wal::WalError::Io(_)) => {}
                other => panic!("an uncheckable head read as no head: {other:?}"),
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }

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
}

#[cfg(test)]
mod archive_tests;

#[cfg(test)]
mod deadline_tests;

#[cfg(test)]
mod shutdown_tests;

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
            let seq = self.inner.append_v4(fence, record, false, prepared, at)?;
            if sync {
                self.sync()?;
            }
            Ok(seq)
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
        fn last_cursor_frame_identity(&self) -> Option<broca_wal::framing::CursorFrameIdentity> {
            self.inner.last_cursor_frame_identity()
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
                    identity: session.clone(),
                    cause: None,
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
                    identity: session.clone(),
                    cause: None,
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
        let error = handle.shutdown().await.unwrap_err();
        assert!(error.to_string().contains("late_close_publication"));
        assert_eq!(
            std::fs::read_dir(root.0.join("open-writers"))
                .unwrap()
                .count(),
            1
        );
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
                    identity: session.clone(),
                    cause: None,
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
        let published = PublishedLog::open(&bytes, &sidecar).unwrap().unwrap();
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
        let published = PublishedLog::open(&bytes, &sidecar).unwrap().unwrap();
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
        let mut wal = FileWal::open_with_recovery(
            &source,
            broca_wal::WalConfig::OVERHAUL,
            store.feature_floor_path(),
        )
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
        assert!(PublishedLog::open(&bytes, &sidecar).unwrap().is_some());
    }
}

#[allow(clippy::too_many_arguments)]
fn writer_loop_with_identity(
    mut wal: Box<dyn Wal + Send>,
    fence: u64,
    rx: std_mpsc::Receiver<WalCommand>,
    mut observer: Option<RecordObserver>,
    poisoned: Arc<Mutex<Option<WalWriteError>>>,
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
    if let Some(obs) = &observer {
        obs.observe_publication(&*wal);
    }
    let mut dirty = false;
    // The sync deadline: the monotonic instant (in ms) by which dirty data must be
    // synced. Set when an append makes a clean writer dirty, and never slid while
    // dirty, so a queue that never empties cannot postpone the sync past it. Cleared
    // whenever the writer goes clean, and also when a sync fails: the writer is then
    // poisoned and must idle until close, not spin on a deadline that can never succeed.
    let mut deadline_at_ms: Option<u64> = None;
    let mut pending_observed: Vec<PendingObservedRecord> = Vec::new();
    let has_tap = observer
        .as_ref()
        .is_some_and(|observer| observer.tap.is_some() || observer.step_events.is_some());

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
                let poison = current_write_error(&poisoned);
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
                        if let Some(obs) = &mut observer {
                            obs.observe_publication(&*wal);
                            if has_tap {
                                for (seq, record, identity) in &pending_observed {
                                    obs.observe_with_identity(
                                        *seq,
                                        fence,
                                        record,
                                        wal.lineage_id(),
                                        *identity,
                                    );
                                }
                            } else {
                                for (seq, record, identity) in pending_observed.drain(..) {
                                    obs.observe_with_identity(
                                        seq,
                                        fence,
                                        &record,
                                        wal.lineage_id(),
                                        identity,
                                    );
                                }
                            }
                        }
                        if has_tap {
                            if let Some(obs) = &mut observer {
                                for (seq, record, _identity) in pending_observed.drain(..) {
                                    obs.observe_tap(
                                        seq,
                                        fence,
                                        wal.lineage_id(),
                                        wal.writer_id(),
                                        &record,
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => {
                        set_write_poison(&poisoned, error);
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
                let poison = current_write_error(&poisoned);
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
                        if let Some(obs) = &mut observer {
                            obs.observe_publication(&*wal);
                            for (seq, record, identity) in pending_observed.drain(..) {
                                obs.observe_with_identity(
                                    seq,
                                    fence,
                                    &record,
                                    wal.lineage_id(),
                                    identity,
                                );
                                if has_tap {
                                    obs.observe_tap(
                                        seq,
                                        fence,
                                        wal.lineage_id(),
                                        wal.writer_id(),
                                        &record,
                                    );
                                }
                            }
                        }
                    }
                    Err(error) => set_write_poison(&poisoned, error),
                }
            }
            if current_poison(&poisoned).is_none() {
                if let Err(error) = close_writer_identity(
                    &mut *wal,
                    writer.take(),
                    fence,
                    &stamp_clock,
                    observer.as_mut(),
                    &durable_head,
                    publication.publisher.as_mut(),
                ) {
                    set_write_poison(&poisoned, error);
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
                                pending_observed.push((
                                    assigned,
                                    record,
                                    wal.last_cursor_frame_identity(),
                                ));
                            }
                        }
                        Err(error) => set_write_poison(&poisoned, error),
                    }
                }
            }
            WalCommand::Sync(ack) => {
                // When no new append has crossed the writer since the last successful
                // fsync, the current durable head is already exact — return it without
                // paying another no-op sync. If the writer is poisoned, still surface the
                // error even though the seqs happen to match.
                let poison = current_write_error(&poisoned);
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
                            if let Some(obs) = &mut observer {
                                obs.observe_publication(&*wal);
                                if has_tap {
                                    for (seq, record, identity) in &pending_observed {
                                        obs.observe_with_identity(
                                            *seq,
                                            fence,
                                            record,
                                            wal.lineage_id(),
                                            *identity,
                                        );
                                    }
                                } else {
                                    for (seq, record, identity) in pending_observed.drain(..) {
                                        obs.observe_with_identity(
                                            seq,
                                            fence,
                                            &record,
                                            wal.lineage_id(),
                                            identity,
                                        );
                                    }
                                }
                            }
                            last_fsynced_seq
                        })
                } else {
                    poison.map_or(Ok(last_fsynced_seq), Err)
                };
                let res = res.inspect_err(|e| {
                    set_write_poison(&poisoned, e.clone());
                    // The writer is poisoned now: drop the deadline with it, so
                    // the loop idles instead of retrying a sync that cannot succeed.
                    deadline_at_ms = None;
                });
                let durable = res.is_ok();
                let _ = ack.send(res);
                if durable && has_tap {
                    if let Some(obs) = &mut observer {
                        for (seq, record, _identity) in pending_observed.drain(..) {
                            obs.observe_tap(seq, fence, wal.lineage_id(), wal.writer_id(), &record);
                        }
                    }
                }
            }
            WalCommand::Shutdown(ack) => {
                let poison = current_write_error(&poisoned);
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
                    if let Some(obs) = &mut observer {
                        obs.observe_publication(&*wal);
                        for (seq, record, identity) in pending_observed.drain(..) {
                            obs.observe_with_identity(
                                seq,
                                fence,
                                &record,
                                wal.lineage_id(),
                                identity,
                            );
                            if has_tap {
                                obs.observe_tap(
                                    seq,
                                    fence,
                                    wal.lineage_id(),
                                    wal.writer_id(),
                                    &record,
                                );
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
                        observer.as_mut(),
                        &durable_head,
                        publication.publisher.as_mut(),
                    )
                });
                let result = result.inspect_err(|error| {
                    set_write_poison(&poisoned, error.clone());
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
    observer: Option<&mut RecordObserver>,
    durable_head: &AtomicU64,
    publisher: Option<&mut broca_wal::head_proof::Publisher>,
) -> Result<(), WalWriteError> {
    if let Some(writer) = writer {
        let (seq, record, synced) =
            writer
                .close(wal, fence, clock.now_ms())
                .map_err(|error| match error {
                    error @ WalError::RelocationIncomplete { .. } => WalWriteError::from_wal(error),
                    other => {
                        WalWriteError::Failed(format!("writer-id/v1: cannot close writer: {other}"))
                    }
                })?;
        let identity = synced.last_cursor_frame_identity();
        let published = if let Some(publisher) = publisher {
            publisher
                .scan_synced(synced)
                .map_err(|error| error.to_string())?;
            // A durable close forbids further appends. Retain its marker on any
            // late exposure so reclaim or the next opener publishes a successor.
            match publisher.publish(|| clock.now_ms()).map_err(|error| error.to_string())? {
                broca_wal::head_proof::PublicationStatus::Published => true,
                broca_wal::head_proof::PublicationStatus::Late { .. } => return Err("visibility-late/v1: late_close_publication; retained open-writer marker requires a successor".into()),
            }
        } else {
            let _ = synced;
            true
        };
        if published {
            durable_head.store(seq, Ordering::Release);
            if let Some(obs) = observer {
                obs.observe_publication(wal);
                obs.observe_with_identity(seq, fence, &record, wal.lineage_id(), identity);
                obs.observe_tap(seq, fence, wal.lineage_id(), wal.writer_id(), &record);
            }
        }
        match writer.unlink() {
            Ok((marker, identity)) => unlink_settled_predecessors(&marker, &identity),
            Err(error) => {
                tracing::warn!(%error, "writer-id/v1: durable writer close succeeded but marker unlink failed; reclaim will remove the marker")
            }
        }
    }
    Ok(())
}

fn unlink_settled_predecessors(marker: &Path, identity: &BindIdentity) {
    use broca_wal::fs_ops;
    use std::io::Read;

    // This writer has published a newer WriterOpened and WriterClosed. Remove only
    // verified predecessor markers while its session lease is still held. A close
    // whose publication fails never reaches this cleanup.
    let Some(directory) = marker.parent() else {
        return;
    };
    let address = session_addr(identity);
    let entries = match fs_ops::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(target: "writer_reclaim", path = %directory.display(), %error, "closed writer could not scan predecessor markers");
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(target: "writer_reclaim", path = %directory.display(), %error, "closed writer could not read a predecessor marker entry");
                continue;
            }
        };
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some((found_address, writer)) = name.split_once('.') else {
            continue;
        };
        if found_address != address
            || writer.len() != 32
            || !writer
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            continue;
        }
        let remove = || -> Result<(), String> {
            if !entry
                .file_type()
                .map_err(|error| error.to_string())?
                .is_file()
            {
                return Err("writer marker is not a regular file".into());
            }
            let mut bytes = Vec::new();
            fs_ops::File::open(&path)
                .map_err(|error| error.to_string())?
                .take(64 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|error| error.to_string())?;
            if bytes.len() > 64 * 1024 {
                return Err("writer marker exceeds the size limit".into());
            }
            if open_writers::decode_writer_marker(&bytes)? != *identity {
                return Err("writer marker identity does not match the closed session".into());
            }
            fs_ops::remove_file(&path).map_err(|error| error.to_string())
        };
        if let Err(error) = remove() {
            // Retain any marker whose validation or removal fails. A later
            // lease-gated reclaim pass must retry the remaining marker.
            tracing::warn!(target: "writer_reclaim", path = %path.display(), %error, "closed writer retained predecessor marker");
        }
    }
}

/// Records the first fatal writer error, which every later append and barrier then reports.
///
/// The `unwrap` on the lock is safe for a reason worth stating, because this is the mechanism
/// that records writer failure and a recorder that could itself fail would leave the writer
/// unable to report why it stopped: a `Mutex` is only poisoned by a panic while its guard is
/// held, and the guarded section here is an `is_none` test and a move. Neither can panic, so
/// this lock cannot be poisoned by its own critical section. Keep it that way — anything
/// fallible added inside the guard makes the failure recorder a second failure source.
fn set_poison(poisoned: &Mutex<Option<WalWriteError>>, error: String) {
    set_write_poison(poisoned, WalWriteError::Failed(error));
}

fn set_write_poison(poisoned: &Mutex<Option<WalWriteError>>, error: WalWriteError) {
    let mut state = poisoned.lock().unwrap();
    if state.is_none() {
        *state = Some(error);
    }
}

/// Reads the recorded writer error. Guarded section is a single clone, so it cannot panic
/// while holding the guard and cannot poison this lock. See `set_poison` for why that matters.
fn current_poison(poisoned: &Mutex<Option<WalWriteError>>) -> Option<String> {
    poisoned.lock().unwrap().as_ref().map(WalWriteError::detail)
}

fn current_write_error(poisoned: &Mutex<Option<WalWriteError>>) -> Option<WalWriteError> {
    poisoned.lock().unwrap().clone()
}
