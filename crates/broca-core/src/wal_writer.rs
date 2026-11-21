//! The file store of the old session log: placement, reads, moves and retention of `.wal`
//! files. Sessions are written by the session store (`broca-session-store`); nothing here
//! starts a writer.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use broca_protocol::BindIdentity;
use broca_wal::{
    FileWal, Wal, WalByteAnchor, WalError, WalRangeRead, WalRecord, DEFAULT_MAX_READ_BYTES,
};

use crate::lease::LeaseHandle;

mod extraction;
mod projection_reader;
mod retention_changes;
pub use projection_reader::ReadOnlyWalReader;
mod projection_gap;
pub use projection_gap::ProjectionAdmission;
pub use projection_gap::{
    session_projection_incomplete, ProjectionRepair, SESSION_PROJECTION_INCOMPLETE,
};
mod session_index;
pub use session_index::{agent_session_ref, legacy_session_ref, record_candidate, SessionRefIndex};

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

mod file_identity;
#[cfg(test)] // On Windows `same_wal_file` calls it inside its own module.
use file_identity::same_native_file_identity;
use file_identity::same_wal_file;

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
                    self.root.parent().unwrap_or(Path::new(".")),
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

    #[test]
    fn writer_records_require_writer_id_and_round_trip() {
        for record in [
            WalRecord::WriterOpened {
                writer_id: [1; 16],
                previous_writer_id: None,
                recovered_through_seq: 0,
                cause: "new".into(),
            },
            WalRecord::WriterClosed { writer_id: [1; 16] },
        ] {
            assert_eq!(record.required_features(), ["writer-id/v1"]);
            assert!(record.is_informational());
            let bytes = broca_wal::framing::encode(1, 1, &record).unwrap();
            assert!(matches!(
                broca_wal::framing::decode_next(&bytes),
                broca_wal::framing::DecodeStep::Record { .. }
            ));
        }
    }
}

#[cfg(test)]
mod archive_tests;

#[cfg(test)]
mod session_class_tests;
