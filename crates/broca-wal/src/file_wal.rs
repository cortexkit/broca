//! `FileWal` — the append-only, fsync-durable WAL log behind the `Wal` trait.
//!
//! Uses sync `std::fs` so fsync semantics are exact; the writer task wraps it with
//! the channel/ack barrier protocol. Replay reads the whole log, discards a TORN
//! trailing record (an incomplete final append), and FAILS LOUD on non-tail
//! corruption (a complete record that fails its digest).

use std::{
    cell::{Cell, RefCell},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::fs_ops::{self, File, OpenOptions};

use crate::writer_tenure::{OwnedWalTenure, TenureError};

use crate::{
    framing::{
        decode_next, encode, encode_lineage, DecodeStep, FrameError, FramedRecord, PreparedV4Frame,
        WalLineageId,
    },
    record::WalRecord,
    state_format::{StateRootError, WalConfig, WalFormat},
};

use broca_protocol::BindIdentity;

pub const DEFAULT_MAX_READ_BYTES: u64 = 1024 * 1024 * 1024;

#[path = "human_prompt_count.rs"]
mod human_prompt_count;

#[cfg(test)]
#[path = "file_wal_v4_tests.rs"]
mod v4_tests;
pub use human_prompt_count::{
    count_human_prompts_in_bytes, read_human_prompt_count, HumanPromptCount,
};

#[cfg(test)]
thread_local! {
    static OPEN_STAGES: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    static OPEN_READ_CAP: Cell<u64> = const { Cell::new(DEFAULT_MAX_READ_BYTES) };
    /// Test-only unwind seam for the owned open path. When set, the owned
    /// constructor panics after recovery, before returning the writer, so
    /// `owned_open_failure_and_panic_release_only_after_close` can exercise
    /// panic cleanup: the log must keep its verified prefix and a fresh
    /// tenure must reopen it. Read with `take`, which also resets the flag,
    /// so a caught panic never arms a later open.
    static OWNED_OPEN_PANIC_AFTER_RECOVERY: Cell<bool> = const { Cell::new(false) };
    /// Counts final barriers performed by the owned consuming close. The
    /// settle itself is not observable in process (an fsync only matters
    /// across a crash), so the close test asserts this counter instead of
    /// trusting `sync_result` alone.
    static OWNED_CLOSE_SETTLES: Cell<u64> = const { Cell::new(0) };
}

fn open_observed(_stage: &'static str) {
    #[cfg(test)]
    OPEN_STAGES.with(|stages| stages.borrow_mut().push(_stage));
}

/// An optional byte offset for starting a later read without decoding the WAL prefix.
///
/// `file_id` fingerprints the opened file's device and inode on Unix. Other platforms
/// currently return no anchors, so the optimization is disabled there rather than relying
/// on a weaker identity. Anchors are derived hints only: every mismatch falls back to a
/// read from byte zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalByteAnchor {
    pub file_id: u64,
    pub seq: u64,
    pub byte_end: u64,
}

/// Records paired with the completeness of the source that supplied them.
///
/// `Complete` DOES NOT MEAN THE SOURCE FILE IS WHOLE. A torn trailing record makes
/// `decode_bytes_seeded` stop at the tear and return the prefix it decoded, and this
/// type reports that prefix as `Complete`; the byte offset where decoding stopped is
/// not carried out of the decoder. That is correct for readers, because discarding a
/// torn tail is what every WAL reader does — but an inference of the form "this file
/// contains no record of kind X" CANNOT rest on it, since the tear may have removed
/// the very record that would disprove it. `crates/broca-core/src/wal_fold.rs` needs
/// exactly that guarantee for its pre-schema age inference and walks the framing
/// itself rather than trusting this variant. Two readers, independently, read the
/// name as whole-file validity before that was noticed.
///
/// `Complete` covers the requested range of an opened, unarchived WAL, not records
/// outside the requested sequence bounds. `Partial` is reserved for a source that
/// knows some of that range is elsewhere. Non-file stores report `Unknown`.
/// There is deliberately no accessor that erases this distinction.
#[derive(Debug, Clone, PartialEq)]
pub enum WalRangeRecords<T> {
    Complete(T),
    Partial(T),
    Absent,
    Unknown(T),
}

/// Records selected by a range read, their byte anchors, and any WAL identity metadata.
///
/// Record access requires matching the completeness returned by [`Self::records`]
/// or [`Self::into_records`]. A public vector would bypass that decision:
///
/// ```compile_fail,E0616
/// let range = broca_wal::WalRangeRead::without_anchors(Vec::new());
/// let records = range.records;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct WalRangeRead {
    records: Vec<FramedRecord>,
    pub next_anchor: Option<WalByteAnchor>,
    pub lineage_id: Option<WalLineageId>,
    /// End of the verified source prefix, including trailing lineage frames, only
    /// when this range includes the source's last record. A torn suffix ends before EOF.
    pub complete_byte_end: Option<u64>,
    /// Whether this read observed a live file or a verified archive member.
    ///
    /// `Some(false)` means the source was absent, `Some(true)` means a source was observed,
    /// and `None` means the result was constructed without determining presence.
    file_present: Option<bool>,
    archive_lookup_missed: bool,
    anchors: Vec<WalByteAnchor>,
}

impl WalRangeRead {
    /// An authoritative miss after consulting archives, unlike legacy live-only absence.
    /// Keep the Absent variant while allowing readers to reject a store-prefix-only answer.
    pub fn absent_after_archive_lookup() -> Self {
        Self {
            file_present: Some(false),
            archive_lookup_missed: true,
            ..Self::without_anchors(Vec::new())
        }
    }

    /// Whether archive resolution ran and found no member. Legacy live-only misses retain
    /// their historical behavior; a failed archive lookup must not become a successful prefix.
    pub fn archive_lookup_missed(&self) -> bool {
        self.archive_lookup_missed
    }

    /// Borrow records without discarding the source's completeness observation.
    pub fn records(&self) -> WalRangeRecords<&[FramedRecord]> {
        match self.file_present {
            Some(true) => WalRangeRecords::Complete(&self.records),
            Some(false) => WalRangeRecords::Absent,
            None => WalRangeRecords::Unknown(&self.records),
        }
    }

    /// Absence carries no records, so it requires a separate policy arm.
    ///
    /// ```compile_fail,E0532
    /// use broca_wal::WalRangeRecords;
    /// fn erase_absence(range: WalRangeRecords<Vec<broca_wal::FramedRecord>>)
    ///     -> Vec<broca_wal::FramedRecord>
    /// {
    ///     match range {
    ///         WalRangeRecords::Complete(records) | WalRangeRecords::Absent(records) => records,
    ///         WalRangeRecords::Partial(records) | WalRangeRecords::Unknown(records) => records,
    ///     }
    /// }
    /// ```
    ///
    /// Take records without discarding the source's completeness observation.
    pub fn into_records(self) -> WalRangeRecords<Vec<FramedRecord>> {
        match self.file_present {
            Some(true) => WalRangeRecords::Complete(self.records),
            Some(false) => WalRangeRecords::Absent,
            None => WalRangeRecords::Unknown(self.records),
        }
    }

    /// Build a range result with one ordered anchor per position the reader can expose.
    ///
    /// Presence stays `None`. This constructor is public and takes in-memory records, so it
    /// cannot know whether a file was behind them; the three callers outside this module are
    /// test doubles that opened nothing. `Some(true)` is set by `select_range` only after
    /// opening a live file or decoding a verified archive member. A constructor that asserted
    /// presence would hand every double a false "the file is there, skip the archive".
    pub fn with_anchors(records: Vec<FramedRecord>, anchors: Vec<WalByteAnchor>) -> Self {
        Self::with_anchors_and_lineage(records, anchors, None)
    }

    fn with_anchors_and_lineage(
        records: Vec<FramedRecord>,
        anchors: Vec<WalByteAnchor>,
        lineage_id: Option<WalLineageId>,
    ) -> Self {
        Self {
            records,
            next_anchor: anchors.last().copied(),
            lineage_id,
            complete_byte_end: None,
            file_present: None,
            archive_lookup_missed: false,
            anchors,
        }
    }

    /// Build a range result with no byte anchors, as used by non-file WAL stores.
    pub fn without_anchors(records: Vec<FramedRecord>) -> Self {
        Self {
            records,
            next_anchor: None,
            lineage_id: None,
            complete_byte_end: None,
            file_present: None,
            archive_lookup_missed: false,
            anchors: Vec::new(),
        }
    }

    /// Return the byte anchor ending exactly at `seq`, when this read minted one.
    pub fn anchor_at(&self, seq: u64) -> Option<WalByteAnchor> {
        self.anchors
            .binary_search_by_key(&seq, |anchor| anchor.seq)
            .ok()
            .map(|index| self.anchors[index])
    }
}

#[derive(Debug)]
pub enum WalError {
    SessionMoving {
        cut_id: String,
    },
    SessionMoved {
        holder: String,
    },
    Io(std::io::Error),
    Frame(FrameError),
    TooLarge {
        max_bytes: u64,
        file_size: u64,
    },
    /// A record's sequence did not progress monotonically. The fence is a lease-tenure stamp,
    /// not a value that this reader compares or gates.
    NonMonotonic {
        prev_seq: u64,
        seq: u64,
    },
    /// One or more complete frames are missing from a decoded WAL run. Valid single-writer WALs
    /// are contiguous, so a gap is corruption rather than a recoverable torn tail.
    SequenceGap {
        expected_seq: u64,
        seq: u64,
    },
    /// Non-tail corruption mid-log — fail loud.
    Corrupt {
        offset: usize,
        source: FrameError,
    },
    /// A complete, digest-verified frame at `offset` needs features this build does not know
    /// (`missing`), so a newer build wrote this WAL. The whole WAL is refused.
    ///
    /// Kept apart from [`WalError::Corrupt`] on purpose: the bytes are intact, so nothing may
    /// treat them as damage to repair, truncate or discard. The remedy is to run a build that
    /// knows the missing features.
    NewerWriter {
        offset: usize,
        ver: u8,
        missing: Vec<String>,
    },
    /// The WAL file changed size between this writer's appends — a FOREIGN writer
    /// appended (or truncated) it. The single-writer invariant ("exactly one writer
    /// appends to this lineage file") is violated; fail loud rather than interleave.
    ForeignWrite {
        /// The file size this writer last left the file at.
        expected: u64,
        /// The file's actual size now (a foreign writer's appends, or a truncation).
        found: u64,
    },
    /// Two complete metadata frames in one WAL claimed different durable identities.
    ConflictingLineageIds {
        first: WalLineageId,
        found: WalLineageId,
    },
    /// The operating system could not provide entropy for a newly-created lineage identity.
    Entropy(getrandom::Error),
    /// A record needs WAL features (`requires`) that the machine's feature floor must list
    /// before any frame uses them, and the floor could not be raised: either no floor file is
    /// configured for this WAL (`path` is `None`) or writing it failed. Nothing was appended.
    FeatureFloor {
        path: Option<PathBuf>,
        requires: Vec<String>,
        reason: String,
    },
    /// The state root's log format could not be decided, or a store refuses to write on it
    /// (see [`StateRootError`]). Nothing was opened or written.
    StateRoot(StateRootError),
    /// A frame of the `offered` format was handed to a log of the `log` format: an overhaul log
    /// writes only the version 4 frames it stamps itself, and a legacy log never writes them.
    /// Nothing was written.
    FrameFormat {
        log: WalFormat,
        offered: WalFormat,
    },
}

impl WalError {
    pub fn into_move_refusal(self) -> Result<crate::migration::MoveRefusal, Self> {
        match self {
            Self::SessionMoving { cut_id } => Ok(crate::migration::MoveRefusal::Moving { cut_id }),
            Self::SessionMoved { holder } => Ok(crate::migration::MoveRefusal::Moved { holder }),
            other @ (Self::Io(_)
            | Self::Frame(_)
            | Self::TooLarge { .. }
            | Self::NonMonotonic { .. }
            | Self::SequenceGap { .. }
            | Self::Corrupt { .. }
            | Self::NewerWriter { .. }
            | Self::ForeignWrite { .. }
            | Self::ConflictingLineageIds { .. }
            | Self::Entropy(_)
            | Self::FeatureFloor { .. }
            | Self::StateRoot(_)
            | Self::FrameFormat { .. }) => Err(other),
        }
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionMoving { cut_id } => write!(f, "session_moving {{cutId: {cut_id}}}"),
            Self::SessionMoved { holder } => write!(f, "session_moved {{holder: {holder}}}"),
            WalError::Io(e) => write!(f, "wal io: {e}"),
            WalError::Frame(e) => write!(f, "wal frame: {e}"),
            WalError::TooLarge {
                max_bytes,
                file_size,
            } => write!(
                f,
                "wal size refused: file size {file_size} exceeds the {max_bytes}-byte safety cap"
            ),
            WalError::NonMonotonic { prev_seq, seq } => {
                write!(f, "wal seq regressed: {seq} after {prev_seq}")
            }
            WalError::SequenceGap { expected_seq, seq } => {
                write!(f, "wal sequence gap: expected {expected_seq}, found {seq}")
            }
            WalError::Corrupt { offset, source } => {
                write!(f, "wal corruption at offset {offset}: {source}")
            }
            WalError::NewerWriter {
                offset,
                ver,
                missing,
            } => write!(
                f,
                "wal written by a newer broca (NewerWriter): the version {ver} frame at offset \
                 {offset} requires features this build does not know: {}",
                missing.join(", ")
            ),
            WalError::ForeignWrite { expected, found } => write!(
                f,
                "wal file changed under this writer (a foreign writer appended/truncated): \
                 expected size {expected}, found {found}"
            ),
            WalError::ConflictingLineageIds { first, found } => write!(
                f,
                "wal contains conflicting lineage ids {first} and {found}"
            ),
            WalError::Entropy(error) => write!(f, "mint WAL lineage id: {error}"),
            WalError::FeatureFloor {
                path,
                requires,
                reason,
            } => write!(
                f,
                "wal append refused: the WAL feature floor {} must list {} before a frame \
                 requiring it is written, and it could not be raised: {reason}",
                path.as_ref().map_or_else(
                    || "(none configured)".to_owned(),
                    |p| p.display().to_string()
                ),
                requires.join(", ")
            ),
            WalError::StateRoot(error) => error.fmt(f),
            WalError::FrameFormat { log, offered } => write!(
                f,
                "wal append refused: a {} log does not write {} frames",
                format_name(*log),
                format_name(*offered)
            ),
        }
    }
}

/// How a refusal names a frame format.
fn format_name(format: WalFormat) -> &'static str {
    match format {
        WalFormat::Legacy => "legacy",
        WalFormat::Overhaul => "version 4 (broca-state/2)",
    }
}

impl std::error::Error for WalError {}

/// How one `FileWal` validates the move fence around its mutations.
///
/// The standalone compatibility path re-checks the fence on every mutation.
/// The owned path checks once, at open, while holding the tenure, and caches
/// the clean verdict for exactly the validated path and session reference; a
/// path or reference transition is a new validation phase. `Closed` is the
/// terminal state left behind when the owned tenure is handed back: no append
/// path runs in it.
///
/// The tenure stored here carries no release handle (minting and the real lock
/// live outside this crate for now), but the ordering rule is already
/// structural: the descriptor field below is declared before `access`, so
/// drop and unwind close the writable handle before tenure teardown.
enum AccessMode {
    /// Standalone path: re-checks the move fence on every mutation and holds
    /// no tenure to cache a verdict under.
    GuardEveryMutation,
    /// Tenure-backed writer: `fenced_path`/`fenced_ref` name the path and
    /// session reference the open-time guard cleared under `tenure`.
    OwnedTenure {
        tenure: OwnedWalTenure,
        fenced_path: PathBuf,
        fenced_ref: String,
    },
    /// The tenure was handed back; the writer accepts no more mutations.
    Closed,
}

fn closed_error() -> WalError {
    WalError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "owned WAL writer is closed",
    ))
}

/// The append-only WAL log. The loop depends on the trait, not the impl.
pub trait Wal {
    /// Append a record at the next seq, stamping it with the lease-tenure `fence`. The fence is
    /// forensic metadata only; single-writer safety comes from the lease's OS lock and this
    /// implementation's file-integrity check. If `sync`, fsync before returning (a barrier
    /// point). Returns the assigned seq.
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError>;

    /// Reuse a frame encoded by the single writer's enqueue path. Implementations without
    /// byte-budget support can keep their ordinary append path.
    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        _frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.append(fence, record, sync)
    }

    /// Optional (total-byte ceiling, current file size, metadata bytes added per record).
    /// Enqueue checks charge the encoded frame plus lineage metadata against this ceiling,
    /// reserving terminal space so refusing a record never makes the log unreadable.
    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        None
    }

    /// The frame format this log writes, as the store that opened it was configured at boot.
    /// A writer handle reads it once, at spawn, to choose how it hands records over: an
    /// overhaul log takes them through [`Wal::append_v4`] only. The default is legacy.
    fn wal_config(&self) -> WalConfig {
        WalConfig::LEGACY
    }

    /// The random identity minted by this overhaul WAL open; never supplied by a caller.
    fn writer_id(&self) -> Option<[u8; 16]> {
        None
    }

    /// Bytes held for this open writer's durable close, separate from the run terminal reserve.
    fn writer_close_reserve(&self) -> u64 {
        0
    }

    /// Append `record` as the version 4 frame `prepared` (built from that same record), stamped
    /// with `appended_ms`, which the caller samples on the thread that calls this, immediately
    /// before the call. The log fills in the sequence, fence and its own `writer_id`, and raises
    /// the feature floor over every name the frame lists before writing it.
    ///
    /// Only an overhaul log writes version 4 frames; the default refuses, as a legacy log does.
    fn append_v4(
        &mut self,
        _fence: u64,
        _record: &WalRecord,
        _sync: bool,
        _prepared: PreparedV4Frame,
        _appended_ms: u64,
    ) -> Result<u64, WalError> {
        Err(WalError::FrameFormat {
            log: WalFormat::Legacy,
            offered: WalFormat::Overhaul,
        })
    }

    fn recovery_byte_cap(&self) -> Option<u64> {
        None
    }

    fn append_budget_scope(&self) -> crate::size_limit::SizeScope {
        crate::size_limit::SizeScope::Session
    }

    /// fsync queued writes without adding a record, including those previously
    /// appended with `sync = false`.
    fn sync(&self) -> Result<(), WalError>;

    /// Replay every committed record in order (torn tail discarded; non-tail
    /// corruption is an error).
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError>;

    /// Records with `seq > after_seq` — the in-WAL tail past the store watermark
    /// (the records the materialized store hasn't projected yet).
    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<FramedRecord>, WalError>;

    /// The highest committed seq (0 if empty).
    fn last_seq(&self) -> u64;

    /// The durable lineage identity stored in this log, if it has one yet.
    ///
    /// A newly created file carries one from open; a legacy file gains one on its first
    /// append. Implementations with no durable identity (in-memory test logs) return `None`,
    /// and callers that need an identity must treat that as "unknown", never invent one.
    fn lineage_id(&self) -> Option<WalLineageId> {
        None
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
struct SyncProbe {
    file_syncs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    dir_syncs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    dir_sync_paths: std::sync::Arc<std::sync::Mutex<Vec<PathBuf>>>,
}

pub struct FileWal {
    path: PathBuf,
    file: File,
    next_seq: u64,
    /// The file size this writer last left the file at (the byte offset after its last
    /// append; the file size at open). Before each append, the writer re-checks the
    /// file's actual size against this: if it changed, a FOREIGN writer touched the file
    /// since this writer's last append, so the append is rejected.
    ///
    /// This is the single-writer FILE-INTEGRITY belt — it enforces "exactly one writer
    /// appends to this lineage file" by physical observation (file size is per-inode, so
    /// a foreign writer's appends via a different handle ARE visible here). The LEASE's OS
    /// advisory lock is the primary guard against two concurrent writers. The `fence` stored
    /// in each record is a lease-tenure stamp for forensic correlation only; FileWal does not
    /// compare it with an authority and does not use it to reject an append. The belt catches
    /// the residual case: a draining old writer appending AFTER a new writer already wrote.
    last_write_end: u64,
    /// The first barrier after each open syncs the WAL's parent directory so the
    /// file's name is durable, including when an earlier process did not sync it.
    dir_needs_sync: Cell<bool>,
    /// Directories that were absent before `create_dir_all`, ordered from the WAL's parent
    /// upward. The first directory-sync pass drains this list after syncing every directory
    /// whose direct child was newly created.
    newly_created_dirs: RefCell<Vec<PathBuf>>,
    /// The frame format this log writes, set by the store that opened it from the boot
    /// decision. Legacy unless [`FileWal::with_wal_config`] says otherwise.
    config: WalConfig,
    /// Minted when the log is configured as an overhaul log: the random 128-bit id stamped in
    /// every version 4 frame this open appends, so a frame names the writer that wrote it.
    writer_id: Option<[u8; 16]>,
    writer_close_reserve: u64,
    lineage_id: Option<WalLineageId>,
    /// The machine's WAL feature floor (see [`crate::floor`]). A record that requires any WAL
    /// feature is appended only after this file lists it; with no floor configured such a
    /// record is refused, because nothing would tell placement that the machine now holds it.
    recovery_cap: u64,
    append_budget: u64,
    budget_scope: crate::size_limit::SizeScope,
    feature_floor: Option<PathBuf>,
    session_ref: String,
    /// Which move-fence discipline this handle follows. Declared after the
    /// descriptor (`file`) so drop and unwind close the writable handle
    /// before tenure teardown, even on the panic path out of open.
    access: AccessMode,
    #[cfg(test)]
    sync_probe: Option<SyncProbe>,
}

/// Create `dir` and any missing ancestors owner-only.
///
/// A WAL holds full prompts, tool arguments and tool results. The request-dump directory in
/// broca-provider is created 0700 for exactly that reason, and it holds a temporary copy of
/// the same content -- so the judgement was already made, applied to the copy, and never
/// carried to the permanent original. `create_dir_all` takes the umask, which is 0022 on a
/// default install, so the durable log landed world-readable.
///
/// Creation-time only, deliberately. Repairing an existing tree means walking every session
/// directory, and the writer's open path -- which also does torn-tail truncation and the
/// directory fsync -- is the wrong place for a filesystem sweep. The repair verb lives in the
/// store-maintenance binary where an operator can run it once.
fn create_private_dir_all(dir: &Path) -> std::io::Result<()> {
    fs_ops::create_dir_all(dir)
}

fn mint_lineage_id() -> Result<WalLineageId, WalError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(WalError::Entropy)?;
    // UUID-shaped presentation is only a convenient encoding; consumers treat the value as opaque.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(WalLineageId::from_bytes(bytes))
}

/// Where an append's bytes come from.
enum FrameSource {
    /// Encode the record here, in this log's format.
    Encode,
    /// A legacy frame the writer handle encoded to measure its budget. Only a legacy log takes
    /// it, after checking it is exactly the frame its sequence, fence and record require.
    Legacy(Vec<u8>),
    /// A version 4 payload the writer handle prepared, and the `appended_ms` its writer thread
    /// sampled. Only an overhaul log takes it, and stamps the header itself.
    V4 {
        prepared: PreparedV4Frame,
        appended_ms: u64,
    },
}

struct ManifestAppend {
    path: PathBuf,
    session_addr: String,
    identity: String,
}

/// Render one parseable diagnostic mapping. The embedded bind identity remains unit-separated.
pub fn format_manifest_line(
    session_addr: &str,
    lineage_id: WalLineageId,
    identity: &str,
) -> std::io::Result<String> {
    if session_addr.contains('\n')
        || session_addr.contains('\r')
        || identity.contains('\n')
        || identity.contains('\r')
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "WAL manifest mapping contains a line break",
        ));
    }
    Ok(format!("{session_addr}  {lineage_id}  {identity}\n"))
}

/// Append one complete manifest record with one operating-system write.
///
/// Multiple daemons can share the append-only sidecar. Formatting before opening the append handle
/// keeps one process's fields from splicing into another process's line. There is deliberately no
/// flush or fsync: this index is reconstructable from SQLite and WAL lineage frames.
fn append_manifest_line(entry: &ManifestAppend, lineage_id: WalLineageId) -> std::io::Result<()> {
    let line = format_manifest_line(&entry.session_addr, lineage_id, &entry.identity)?;
    let mut options = OpenOptions::new();
    options.append(true).create(true);
    let mut file = options.open(&entry.path)?;
    let written = file.write(line.as_bytes())?;
    if written != line.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            format!(
                "short WAL manifest append: wrote {written} of {} bytes",
                line.len()
            ),
        ));
    }
    Ok(())
}

impl FileWal {
    fn unstarted(&self) -> Result<bool, WalError> {
        if self.next_seq == 1 {
            return Ok(true);
        }
        Ok(self.config.is_overhaul()
            && self.replay()?.iter().all(|frame| {
                matches!(
                    frame.record,
                    WalRecord::WriterOpened { .. } | WalRecord::WriterClosed { .. }
                )
            }))
    }

    /// A status/warm open can precede the first send. Move only an unstarted file,
    /// under its caller's session lease, before the first record fixes its placement.
    ///
    /// In the owned mode the destination's move sidecars are revalidated before
    /// the rename: the cached open-time verdict covers the source path, never a
    /// destination the guard has not cleared. The standalone path keeps its
    /// existing behavior and re-checks on every later append instead.
    pub fn relocate_unstarted(&mut self, destination: &Path) -> Result<(), WalError> {
        match &self.access {
            AccessMode::Closed => return Err(closed_error()),
            AccessMode::OwnedTenure { .. } => {
                crate::migration::guard(destination, &self.session_ref, true)?;
            }
            AccessMode::GuardEveryMutation => {}
        }
        if self.path == destination {
            return Ok(());
        }
        if !self.unstarted()? {
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot relocate a started session WAL",
            )));
        }
        if wal_file_exists(destination)? {
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "conflicting first-send WAL placement",
            )));
        }
        let parent = destination.parent().ok_or_else(|| {
            WalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WAL destination has no directory",
            ))
        })?;
        let source_parent = self.path.parent().unwrap().to_path_buf();
        crate::migration::durable_dir(parent.parent().unwrap_or(parent), parent)
            .map_err(WalError::Io)?;
        fs_ops::rename(&self.path, destination).map_err(WalError::Io)?;
        self.path = destination.to_path_buf();
        self.dir_needs_sync.set(true);
        self.sync()?;
        self.refresh_owned_cache();
        crate::migration::sync_dir(&source_parent).map_err(WalError::Io)
    }

    /// Mark the current path and session reference as the verdict the owned
    /// open-time guard cleared. A path or reference transition invalidates the
    /// cache, and the next owned append revalidates before writing.
    fn refresh_owned_cache(&mut self) {
        if let AccessMode::OwnedTenure {
            fenced_path,
            fenced_ref,
            ..
        } = &mut self.access
        {
            fenced_path.clone_from(&self.path);
            fenced_ref.clone_from(&self.session_ref);
        }
    }

    /// Borrow the held tenure, if this handle runs in the owned mode.
    fn owned_tenure(&self) -> Option<&OwnedWalTenure> {
        match &self.access {
            AccessMode::OwnedTenure { tenure, .. } => Some(tenure),
            AccessMode::GuardEveryMutation | AccessMode::Closed => None,
        }
    }

    /// Hand back the held tenure and leave the writer terminal: the mode
    /// becomes `Closed`, so no append path runs after the handoff even if a
    /// caller kept another handle to the same value (only the facade holds
    /// owned handles, and closing it consumes the facade).
    fn take_owned_tenure(&mut self) -> Option<OwnedWalTenure> {
        match std::mem::replace(&mut self.access, AccessMode::Closed) {
            AccessMode::OwnedTenure { tenure, .. } => Some(tenure),
            previous => {
                self.access = previous;
                None
            }
        }
    }
    /// Open (creating if absent) the WAL at `path`, recovering `next_seq` from the
    /// last committed record. Creation writes a fresh lineage identity; an existing legacy file
    /// remains unidentified until its next append. A torn trailing frame (an append that crashed
    /// mid-write, never fsynced as complete) is TRUNCATED away on open, so the file ends exactly at
    /// the last valid frame before any new append.
    ///
    /// Truncating is load-bearing, not cosmetic: if the torn bytes were left in place, the
    /// next append would write AFTER them, and a later full replay would then encounter a
    /// torn frame that is no longer at the tail — which fails loud (mid-stream corruption),
    /// bricking an otherwise-recoverable session. A crash during an append followed by a
    /// resume-append is exactly the core recovery path, so this must be clean.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, WalError> {
        Self::open_inner(
            path.into(),
            None,
            String::new(),
            AccessMode::GuardEveryMutation,
            None,
        )
    }

    // Callers of this test seam are all `#[cfg(unix)]` (the directory-fsync probe tests), so on
    // Windows it is callerless — allow it there rather than fail the `-D warnings` build.
    #[cfg(test)]
    #[cfg_attr(not(unix), allow(dead_code))]
    fn open_with_probe(path: impl Into<PathBuf>, sync_probe: SyncProbe) -> Result<Self, WalError> {
        Self::open_inner(
            path.into(),
            None,
            String::new(),
            AccessMode::GuardEveryMutation,
            Some(sync_probe),
        )
    }

    /// Open a WAL and record its diagnostic address when this call creates the file.
    ///
    /// The manifest is never consulted while opening a WAL. Its append is best-effort because the
    /// WAL and its embedded lineage frame remain the source of truth when this sidecar is absent,
    /// stale, or corrupt.
    pub fn open_with_manifest(
        path: impl Into<PathBuf>,
        manifest_path: impl Into<PathBuf>,
        session_addr: impl Into<String>,
        identity: impl Into<String>,
    ) -> Result<Self, WalError> {
        Self::open_with_manifest_and_session_ref(
            path,
            manifest_path,
            session_addr,
            identity,
            String::new(),
        )
    }

    /// Open with the shared session reference already available to migration fencing.
    /// Supplying it after open is too late to use the per-reference cut index.
    pub fn open_with_manifest_and_session_ref(
        path: impl Into<PathBuf>,
        manifest_path: impl Into<PathBuf>,
        session_addr: impl Into<String>,
        identity: impl Into<String>,
        session_ref: String,
    ) -> Result<Self, WalError> {
        Self::open_inner(
            path.into(),
            Some(ManifestAppend {
                path: manifest_path.into(),
                session_addr: session_addr.into(),
                identity: identity.into(),
            }),
            session_ref,
            AccessMode::GuardEveryMutation,
            None,
        )
    }

    fn open_inner(
        path: PathBuf,
        manifest: Option<ManifestAppend>,
        session_ref: String,
        access: AccessMode,
        #[cfg(test)] sync_probe: Option<SyncProbe>,
        #[cfg(not(test))] _sync_probe: Option<()>,
    ) -> Result<Self, WalError> {
        crate::migration::guard(&path, &session_ref, true)?;
        #[cfg(windows)]
        fs_ops::validate_if_exists(&path).map_err(WalError::Io)?;
        let mut newly_created_dirs = Vec::new();
        if let Some(parent) = path.parent() {
            let mut candidate = parent;
            while !candidate.as_os_str().is_empty()
                && !fs_ops::try_exists(candidate).map_err(WalError::Io)?
            {
                newly_created_dirs.push(candidate.to_path_buf());
                let Some(next) = candidate.parent() else {
                    break;
                };
                candidate = next;
            }
            create_private_dir_all(parent).map_err(WalError::Io)?;
        }
        // Recover the valid records and metadata plus the byte offset where they end (the start of
        // any torn tail, or the whole file if intact).
        open_observed("replay");
        #[cfg(test)]
        let max_read_bytes = OPEN_READ_CAP.with(Cell::get);
        #[cfg(not(test))]
        let max_read_bytes = DEFAULT_MAX_READ_BYTES;
        let recovered = match File::open(&path) {
            Ok(file) => {
                let size = file.metadata().map_err(WalError::Io)?.len();
                if size > max_read_bytes {
                    return Err(WalError::TooLarge {
                        max_bytes: max_read_bytes,
                        file_size: size,
                    });
                }
                scan_prefix(
                    std::io::BufReader::new(file),
                    max_read_bytes,
                    |_| None::<()>,
                )?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                scan_prefix(std::io::empty(), max_read_bytes, |_| None::<()>)?
            }
            Err(error) => return Err(WalError::Io(error)),
        };
        let valid_end = recovered.valid_end;
        let recovered_lineage_id = recovered.lineage_id;
        // Seq is 1-BASED: the first record is seq 1, so a watermark of 0
        // unambiguously means "nothing projected yet" and `tail_after_seq(0)`
        // includes the first record (the cold-start merge case).
        let next_seq = recovered.last_seq.map(|seq| seq + 1).unwrap_or(1);
        // If the file is longer than the last valid record, a torn tail exists — truncate it
        // so the file ends exactly at the last valid record, BEFORE opening the append
        // handle. The truncation uses a dedicated WRITE handle: an append-mode handle
        // (opened below) can't `set_len` on Windows (a FILE_APPEND_DATA handle lacks the
        // write-data right SetEndOfFile needs → "Access is denied"); a plain write handle
        // has it. A clean cut (no torn bytes) makes valid_end == the file size, so this is
        // skipped then.
        if let Ok(meta) = fs_ops::metadata(&path) {
            if meta.len() > valid_end {
                open_observed("truncate");
                let trunc = OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(WalError::Io)?;
                trunc.set_len(valid_end).map_err(WalError::Io)?;
                trunc.sync_all().map_err(WalError::Io)?; // durable before appending after it
            }
        }
        let mut create_opts = OpenOptions::new();
        create_opts.read(true).append(true).create_new(true);
        let (mut file, created) = match create_opts.open(&path) {
            Ok(file) => (file, true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&path)
                    .map_err(WalError::Io)?,
                false,
            ),
            Err(e) => return Err(WalError::Io(e)),
        };
        let mut lineage_id = recovered_lineage_id;
        // Creation is the identity boundary. Legacy files deliberately remain unidentified on
        // open and receive their first metadata frame only when append performs the next write.
        let last_write_end = if created {
            let minted = mint_lineage_id()?;
            let metadata = encode_lineage(minted);
            file.write_all(&metadata).map_err(WalError::Io)?;
            file.flush().map_err(WalError::Io)?;
            lineage_id = Some(minted);
            if let Some(manifest) = &manifest {
                if let Err(error) = append_manifest_line(manifest, minted) {
                    tracing::warn!(
                        target: "archive",
                        "cannot append diagnostic WAL manifest {} for {}: {error}; \
                         continuing with the WAL as source of truth",
                        manifest.path.display(),
                        manifest.session_addr
                    );
                }
            }
            valid_end + metadata.len() as u64
        } else {
            valid_end
        };
        Ok(Self {
            path,
            file,
            next_seq,
            last_write_end,
            dir_needs_sync: Cell::new(true),
            newly_created_dirs: RefCell::new(newly_created_dirs),
            config: WalConfig::LEGACY,
            writer_id: None,
            writer_close_reserve: 0,
            lineage_id,
            recovery_cap: DEFAULT_MAX_READ_BYTES,
            append_budget: DEFAULT_MAX_READ_BYTES,
            budget_scope: crate::size_limit::SizeScope::Session,
            feature_floor: None,
            session_ref,
            access,
            #[cfg(test)]
            sync_probe,
        })
    }

    /// Name the feature floor this WAL raises before it appends a record that requires a WAL
    /// feature. The store that owns the WAL tree sets it to the floor in its state root.
    pub fn with_feature_floor(mut self, floor: impl Into<PathBuf>) -> Self {
        self.feature_floor = Some(floor.into());
        self
    }

    /// Write this log in `config`'s format, as decided once at boot for its state root. An
    /// overhaul log mints its random `writer_id` here, so each open stamps its own; it then writes
    /// only version 4 frames it stamps itself and refuses caller-encoded bytes.
    pub fn with_wal_config(mut self, config: WalConfig) -> Result<Self, WalError> {
        self.writer_id = match config.format {
            WalFormat::Legacy => None,
            WalFormat::Overhaul => {
                let mut id = [0u8; 16];
                getrandom::fill(&mut id).map_err(WalError::Entropy)?;
                Some(id)
            }
        };
        self.config = config;
        Ok(self)
    }

    /// The first barrier syncs the file, its parent, and parents of directories
    /// created by this open. Unix errors fail the barrier and keep every directory
    /// in that bottom-up list queued for the next attempt.
    fn fsync(&self) -> Result<(), WalError> {
        self.file.sync_all().map_err(WalError::Io)?;
        #[cfg(test)]
        if let Some(probe) = &self.sync_probe {
            probe
                .file_syncs
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if self.dir_needs_sync.get() {
            let sync_path = |path: &Path| {
                if path.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    path.to_path_buf()
                }
            };
            let mut sync_dirs = Vec::new();
            if let Some(parent) = self.path.parent() {
                // The WAL file's name lives in its immediate parent.
                sync_dirs.push(sync_path(parent));
            }
            for created in self.newly_created_dirs.borrow().iter() {
                // A newly-created directory's name lives in its own parent. The created list is
                // bottom-up, so these parents preserve the required durability order.
                if let Some(parent) = created.parent() {
                    let parent = sync_path(parent);
                    if sync_dirs.last() != Some(&parent) {
                        sync_dirs.push(parent);
                    }
                }
            }
            for dir_path in sync_dirs {
                #[cfg(test)]
                if let Some(probe) = &self.sync_probe {
                    // Count directory-barrier attempts, including the explicit non-Unix no-op.
                    probe
                        .dir_syncs
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    probe.dir_sync_paths.lock().unwrap().push(dir_path.clone());
                }
                crate::directory_sync::sync_dir(&dir_path).map_err(WalError::Io)?;
            }
            self.newly_created_dirs.borrow_mut().clear();
            self.dir_needs_sync.set(false);
        }
        Ok(())
    }
}

impl FileWal {
    pub fn with_session_ref(mut self, session_ref: String) -> Self {
        self.session_ref = session_ref;
        self
    }
    /// Restrict the total byte ceiling without allowing it to exceed recovery's cap.
    pub fn with_recovery_cap(mut self, max_bytes: u64) -> Self {
        self.recovery_cap = max_bytes.min(DEFAULT_MAX_READ_BYTES);
        self.append_budget = self.recovery_cap;
        self
    }

    /// Apply a total-byte ceiling and choose `retained_session_too_large` rather
    /// than `session_too_large` for the writer's typed refusal terminal.
    pub fn with_append_budget(mut self, max_bytes: u64) -> Self {
        self.append_budget = max_bytes.min(self.recovery_cap);
        self.budget_scope = crate::size_limit::SizeScope::RetainedSession;
        self
    }

    /// Assemble the version 4 frame `prepared` at `seq`, with this open's `writer_id`, and hand
    /// back the names it requires.
    fn stamp_v4(
        &self,
        seq: u64,
        fence: u64,
        prepared: PreparedV4Frame,
        appended_ms: u64,
    ) -> Result<(Vec<u8>, std::borrow::Cow<'static, [&'static str]>), WalError> {
        let writer_id = self
            .writer_id
            .expect("an overhaul log mints its writer id when it is configured");
        let frame = prepared
            .stamp(crate::framing::v4::Metadata {
                seq,
                fence,
                appended_ms,
                writer_id,
            })
            .map_err(WalError::Frame)?;
        Ok((frame, prepared.requires().to_vec().into()))
    }

    fn append_frame(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        source: FrameSource,
    ) -> Result<u64, WalError> {
        // One validation phase per tenure: the open-time guard cleared the
        // fenced path and reference under the held tenure, so owned appends
        // skip the repeated migration probes while that verdict still covers
        // this handle. A path or reference transition is a new phase and
        // revalidates before writing. The standalone path has no tenure to
        // cache under and re-checks on every mutation, as before.
        enum Phase {
            RefuseClosed,
            Guard,
            RefreshGuard,
            Cached,
        }
        let phase = match &self.access {
            AccessMode::Closed => Phase::RefuseClosed,
            AccessMode::GuardEveryMutation => Phase::Guard,
            AccessMode::OwnedTenure {
                fenced_path,
                fenced_ref,
                ..
            } if *fenced_path == self.path && *fenced_ref == self.session_ref => Phase::Cached,
            AccessMode::OwnedTenure { .. } => Phase::RefreshGuard,
        };
        match phase {
            Phase::RefuseClosed => return Err(closed_error()),
            Phase::Guard | Phase::RefreshGuard => {
                crate::migration::guard(&self.path, &self.session_ref, true)?;
            }
            Phase::Cached => {}
        }
        if matches!(phase, Phase::RefreshGuard) {
            self.refresh_owned_cache();
        }
        // Single-writer file-integrity check: the file must be exactly where THIS writer
        // last left it. If its size changed, a foreign writer appended (or truncated) it
        // since our last append (file size is per-inode, so a foreign handle's writes are
        // visible here) — reject BEFORE writing, so we never interleave our bytes after a
        // foreign append. `!=` not `<`: a size DECREASE (truncation of an append-only
        // file) is equally anomalous. The append-only fd would otherwise blindly write at
        // the kernel's current end.
        let current = self.file.metadata().map_err(WalError::Io)?.len();
        if current != self.last_write_end {
            return Err(WalError::ForeignWrite {
                expected: self.last_write_end,
                found: current,
            });
        }
        if self.budget_scope == crate::size_limit::SizeScope::RetainedSession
            && matches!(
                record,
                WalRecord::RunStarted {
                    retention: None,
                    ..
                }
            )
            && self.unstarted()?
        {
            self.budget_scope = crate::size_limit::SizeScope::Session;
            self.append_budget = self.recovery_cap;
        }
        let seq = self.next_seq;
        let preencoded = matches!(source, FrameSource::Legacy(_));
        // The bytes this append writes, and every name the machine's feature floor must list
        // before they exist. A legacy log writes exactly what it always has. An overhaul log
        // writes only version 4 frames it stamps here, from a payload the shared codec built:
        // caller-encoded bytes are refused, and nothing falls back to the legacy encoding.
        let (record_bytes, requires): (Vec<u8>, std::borrow::Cow<'static, [&'static str]>) =
            match (self.config.format, source) {
                (WalFormat::Legacy, FrameSource::Encode) => (
                    encode(seq, fence, record).map_err(WalError::Frame)?,
                    record.required_features().into(),
                ),
                (WalFormat::Legacy, FrameSource::Legacy(frame)) => {
                    (frame, record.required_features().into())
                }
                // A direct append, one that did not come through a writer handle, is stamped
                // with the time it reaches this log.
                (WalFormat::Overhaul, FrameSource::Encode) => self.stamp_v4(
                    seq,
                    fence,
                    PreparedV4Frame::prepare(record).map_err(WalError::Frame)?,
                    crate::framing::appended_ms_now(),
                )?,
                (
                    WalFormat::Overhaul,
                    FrameSource::V4 {
                        prepared,
                        appended_ms,
                    },
                ) => self.stamp_v4(seq, fence, prepared, appended_ms)?,
                (log @ WalFormat::Overhaul, FrameSource::Legacy(_)) => {
                    return Err(WalError::FrameFormat {
                        log,
                        offered: WalFormat::Legacy,
                    })
                }
                (log @ WalFormat::Legacy, FrameSource::V4 { .. }) => {
                    return Err(WalError::FrameFormat {
                        log,
                        offered: WalFormat::Overhaul,
                    })
                }
            };
        let overhead = crate::framing::LINEAGE_FRAME_LEN as u64
            * if self.lineage_id.is_none() && current > 0 {
                2
            } else {
                1
            };
        let lifecycle = self.config.is_overhaul()
            && matches!(
                record,
                WalRecord::WriterOpened { .. } | WalRecord::WriterClosed { .. }
            );
        let close_reserve = match record {
            WalRecord::WriterOpened { writer_id, .. } if lifecycle => {
                PreparedV4Frame::prepare(&WalRecord::WriterClosed {
                    writer_id: *writer_id,
                })
                .map_err(WalError::Frame)?
                .frame_len() as u64
                    + overhead
            }
            WalRecord::WriterClosed { .. } if lifecycle => 0,
            _ => self.writer_close_reserve,
        };
        // Lifecycle barriers spend their own held bytes, not another run terminal's reserve.
        // Every ordinary append, including RunFinished, must leave the outstanding close intact.
        let reserve = if lifecycle || matches!(record, WalRecord::RunFinished { .. }) {
            0
        } else {
            crate::size_limit::reserve_bytes(overhead)
        };
        let required = current
            .saturating_add(record_bytes.len() as u64)
            .saturating_add(overhead)
            .saturating_add(reserve)
            .saturating_add(close_reserve);
        if required > self.append_budget {
            return Err(WalError::TooLarge {
                max_bytes: self.append_budget,
                file_size: required,
            });
        }
        // A legacy pre-encoded frame is checked to be exactly the one its sequence, fence and
        // record require. A version 4 frame was stamped above from the codec's own payload.
        if preencoded {
            crate::framing::validate_preencoded(&record_bytes, seq, fence, record)
                .map_err(WalError::Frame)?;
        }
        // Raise the floor BEFORE the frame exists, and refuse the append if it cannot be
        // raised. Placement reads the floor to decide which builds may run on this machine;
        // a gated frame written first would be data it does not know about if the process
        // died before the floor caught up. Every version 4 frame names `frame-stamps/v1`, so
        // on an overhaul log this runs before every append.
        if !requires.is_empty() {
            let refuse = |path: Option<PathBuf>, reason: String| WalError::FeatureFloor {
                path,
                requires: requires.iter().map(|name| (*name).to_owned()).collect(),
                reason,
            };
            let Some(floor) = self.feature_floor.clone() else {
                return Err(refuse(None, "no feature floor is configured".into()));
            };
            crate::floor::raise_floor(&floor, &requires)
                .map_err(|error| refuse(Some(floor), error.to_string()))?;
        }
        let (lineage_id, minted_for_legacy) = match self.lineage_id {
            Some(lineage_id) => (lineage_id, false),
            None => {
                let minted = mint_lineage_id()?;
                self.lineage_id = Some(minted);
                (minted, true)
            }
        };
        let lineage_bytes = encode_lineage(lineage_id);
        let extra_metadata_len =
            usize::from(minted_for_legacy && current > 0) * lineage_bytes.len();
        let mut bytes =
            Vec::with_capacity(record_bytes.len() + lineage_bytes.len() + extra_metadata_len);
        // A legacy WAL has records but no metadata. Put its newly-minted identity before the next
        // record so even a torn record leaves a complete identity frame for the recovery open.
        if minted_for_legacy && current > 0 {
            bytes.extend_from_slice(&lineage_bytes);
        }
        bytes.extend_from_slice(&record_bytes);
        // A trailing identity frame lets anchored reads recover lineage identity from the
        // suffix starting at anchor.byte_end without rescanning the file head. Omitting it
        // would return lineage_id: None and disable the store's transcript-integrity check
        // on the incremental path. Any space optimization must preserve that evidence.
        bytes.extend_from_slice(&lineage_bytes);
        self.file.write_all(&bytes).map_err(WalError::Io)?;
        self.file.flush().map_err(WalError::Io)?;
        if sync {
            self.fsync()?;
        }
        self.next_seq = seq + 1;
        self.writer_close_reserve = close_reserve;
        // Advance our high-water mark past the bytes we just wrote. Only OUR appends move
        // this, so "size unchanged since our last append" ⟺ no foreign write.
        self.last_write_end = current + bytes.len() as u64;
        Ok(seq)
    }
}

impl Wal for FileWal {
    fn writer_close_reserve(&self) -> u64 {
        self.writer_close_reserve
    }
    fn writer_id(&self) -> Option<[u8; 16]> {
        self.writer_id
    }
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.append_frame(fence, record, sync, FrameSource::Encode)
    }

    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.append_frame(fence, record, sync, FrameSource::Legacy(frame))
    }

    fn append_v4(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        prepared: PreparedV4Frame,
        appended_ms: u64,
    ) -> Result<u64, WalError> {
        self.append_frame(
            fence,
            record,
            sync,
            FrameSource::V4 {
                prepared,
                appended_ms,
            },
        )
    }

    fn wal_config(&self) -> WalConfig {
        self.config
    }

    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        Some((
            self.append_budget,
            self.last_write_end,
            crate::framing::LINEAGE_FRAME_LEN as u64
                * if self.lineage_id.is_none() && self.last_write_end > 0 {
                    2
                } else {
                    1
                },
        ))
    }

    fn recovery_byte_cap(&self) -> Option<u64> {
        Some(self.recovery_cap)
    }

    fn append_budget_scope(&self) -> crate::size_limit::SizeScope {
        self.budget_scope
    }

    fn sync(&self) -> Result<(), WalError> {
        self.fsync()
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        replay_path(&self.path, DEFAULT_MAX_READ_BYTES)
    }

    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(replay_path(&self.path, DEFAULT_MAX_READ_BYTES)?
            .into_iter()
            .filter(|r| r.seq > after_seq)
            .collect())
    }

    fn last_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    fn lineage_id(&self) -> Option<WalLineageId> {
        self.lineage_id
    }
}

/// Why a tenure-backed writer open refused before mutating anything.
#[derive(Debug)]
pub enum OwnedOpenCause {
    /// The tenure is bound to a different session than the requested one.
    /// The check runs before any filesystem touch, so a wrong tenure changes
    /// no bytes or directory entries.
    Tenure(TenureError),
    /// The one-time move fence or a recovery/creation step refused. The guard
    /// runs before directory creation, torn-tail truncation, lineage
    /// initialization and the first append, so a move-fence refusal creates
    /// nothing; a later recovery or creation error can follow filesystem
    /// changes (created directories, a truncated torn tail, written lineage).
    Wal(WalError),
}

/// A refused tenure-backed open. The tenure is consumed on both paths: on
/// failure the descriptor (if any) is closed before the tenure is released,
/// and the caller opens again with a freshly acquired tenure.
#[derive(Debug)]
pub struct OwnedOpenError {
    cause: OwnedOpenCause,
}

impl OwnedOpenError {
    /// Which check refused the open.
    pub fn cause(&self) -> &OwnedOpenCause {
        &self.cause
    }
}

impl std::fmt::Display for OwnedOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.cause {
            OwnedOpenCause::Tenure(error) => write!(f, "owned WAL open refused: {error}"),
            OwnedOpenCause::Wal(error) => write!(f, "owned WAL open refused: {error}"),
        }
    }
}

impl std::error::Error for OwnedOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            OwnedOpenCause::Tenure(error) => Some(error),
            OwnedOpenCause::Wal(error) => Some(error),
        }
    }
}

/// The settled result of consuming an owned writer. The final barrier ran
/// under the still-held tenure, and the tenure below is that same still-held
/// owner, returned only after the writable descriptor is gone. Even when the
/// final sync fails, no live writable handle survives alongside it.
pub struct OwnedCloseOutcome {
    /// The final barrier's outcome. A failure is reported, not retried: the
    /// writer is disabled either way.
    pub sync_result: Result<(), WalError>,
    /// The tenure this writer held, handed back after teardown.
    pub tenure: OwnedWalTenure,
}

/// A tenure-backed writable WAL: the only append path that may skip the
/// per-append move fence.
///
/// The open-time guard cleared the log's path and session reference while
/// holding `tenure`, and this facade keeps the descriptor and the tenure
/// together until the consuming close hands the tenure back. Appends stamp
/// the tenure's epoch as their forensic fence: unlike [`Wal::append`], there
/// is no fence parameter, so a caller cannot supply an unrelated integer and
/// mistake it for authority. Comparing the stamp never substitutes for
/// holding this tenure.
///
/// Nothing mints tenures in production yet (the real lock acquisition lands
/// with the store opener), so this path is test-only until then. Readers stay
/// lease-free: use [`replay_file`] and the range readers instead of opening a
/// writer to inspect a log.
///
/// ```compile_fail,E0061
/// fn open_needs_a_tenure(
///     path: std::path::PathBuf,
///     session: &broca_protocol::BindIdentity,
/// ) {
///     let _wal = broca_wal::file_wal::OwnedWriterWal::open(
///         path,
///         session,
///         "session-ref".to_string(),
///     );
/// }
/// ```
///
/// ```compile_fail,E0061
/// fn append_takes_no_caller_fence(
///     wal: &mut broca_wal::file_wal::OwnedWriterWal,
///     record: &broca_wal::WalRecord,
/// ) {
///     wal.append(7, record, true).unwrap();
/// }
/// ```
pub struct OwnedWriterWal {
    wal: FileWal,
}

impl OwnedWriterWal {
    /// Open (creating if absent) the WAL at `path` for `session`, bound to
    /// `tenure`, and recover it exactly like the standalone open.
    ///
    /// The tenure's session binding is checked before touching the
    /// filesystem; the real-reference cut/moved/moving check runs once,
    /// before any writable open, recovery truncation, lineage initialization
    /// or append. `session_ref` must be the move-fence reference derived from
    /// `session` (today `cortexkit_role_llm_runner::session_ref`, derived by
    /// the store that owns the lease domain). Deriving it inside this crate
    /// would need the role crate as a new dependency of `broca-wal`, so the
    /// caller passes both: the tenure check binds the identity while the
    /// guard uses the reference.
    pub fn open(
        path: impl Into<PathBuf>,
        session: &BindIdentity,
        session_ref: impl Into<String>,
        tenure: OwnedWalTenure,
    ) -> Result<Self, OwnedOpenError> {
        Self::open_owned(path.into(), None, session, session_ref.into(), tenure)
    }

    /// Open with the diagnostic manifest this call records when it creates
    /// the file. The manifest stays a best-effort sidecar: it is never
    /// consulted while opening, exactly like the standalone variant.
    pub fn open_with_manifest(
        path: impl Into<PathBuf>,
        manifest_path: impl Into<PathBuf>,
        session_addr: impl Into<String>,
        identity: impl Into<String>,
        session: &BindIdentity,
        session_ref: impl Into<String>,
        tenure: OwnedWalTenure,
    ) -> Result<Self, OwnedOpenError> {
        Self::open_owned(
            path.into(),
            Some(ManifestAppend {
                path: manifest_path.into(),
                session_addr: session_addr.into(),
                identity: identity.into(),
            }),
            session,
            session_ref.into(),
            tenure,
        )
    }

    fn open_owned(
        path: PathBuf,
        manifest: Option<ManifestAppend>,
        session: &BindIdentity,
        session_ref: String,
        tenure: OwnedWalTenure,
    ) -> Result<Self, OwnedOpenError> {
        // Bind before touching the filesystem: a wrong tenure refuses without
        // creating directories or files, truncating a torn tail, or writing
        // lineage. The tenure is consumed on both paths; on failure the
        // half-built descriptor (if any) drops before the tenure is released.
        if let Err(error) = tenure.require_session(session) {
            return Err(OwnedOpenError {
                cause: OwnedOpenCause::Tenure(error),
            });
        }
        let fenced_path = path.clone();
        let fenced_ref = session_ref.clone();
        let access = AccessMode::OwnedTenure {
            tenure,
            fenced_path,
            fenced_ref,
        };
        let wal =
            FileWal::open_inner(path, manifest, session_ref, access, None).map_err(|error| {
                OwnedOpenError {
                    cause: OwnedOpenCause::Wal(error),
                }
            })?;
        #[cfg(test)]
        OWNED_OPEN_PANIC_AFTER_RECOVERY.with(|flag| {
            if flag.take() {
                panic!("owned open probe: panic after recovery, before returning the writer");
            }
        });
        Ok(Self { wal })
    }

    /// Append a record at the next seq, stamped with the tenure's epoch.
    pub fn append(&mut self, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let fence = self.epoch();
        self.wal
            .append_frame(fence, record, sync, FrameSource::Encode)
    }

    /// Append a frame encoded by the single writer's enqueue path, stamped
    /// with the tenure's epoch like [`Self::append`].
    pub fn append_preencoded(
        &mut self,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        let fence = self.epoch();
        self.wal
            .append_frame(fence, record, sync, FrameSource::Legacy(frame))
    }

    /// Move an unstarted log before its first record fixes its placement.
    /// The destination's move sidecars are revalidated before the rename,
    /// and the cached verdict follows the log to its new path.
    pub fn relocate_unstarted(&mut self, destination: &Path) -> Result<(), WalError> {
        self.wal.relocate_unstarted(destination)
    }

    /// Name the feature floor this writer raises before appending a gated record.
    pub fn with_feature_floor(mut self, floor: impl Into<PathBuf>) -> Self {
        self.wal = self.wal.with_feature_floor(floor);
        self
    }

    /// Restrict the total byte ceiling without allowing it to exceed recovery's cap.
    pub fn with_recovery_cap(mut self, max_bytes: u64) -> Self {
        self.wal = self.wal.with_recovery_cap(max_bytes);
        self
    }

    /// Apply a total byte ceiling and choose `retained_session_too_large`
    /// for the writer's typed refusal terminal.
    pub fn with_append_budget(mut self, max_bytes: u64) -> Self {
        self.wal = self.wal.with_append_budget(max_bytes);
        self
    }

    /// fsync queued writes without adding a record.
    pub fn sync(&self) -> Result<(), WalError> {
        self.wal.fsync()
    }

    /// Replay every committed record in order (torn tail discarded; non-tail
    /// corruption is an error).
    pub fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        self.wal.replay()
    }

    /// Records with `seq > after_seq`.
    pub fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        self.wal.tail_after_seq(after_seq)
    }

    /// The highest committed seq (0 if empty).
    pub fn last_seq(&self) -> u64 {
        self.wal.last_seq()
    }

    /// The durable lineage identity stored in this log, if it has one yet.
    pub fn lineage_id(&self) -> Option<WalLineageId> {
        self.wal.lineage_id()
    }

    /// The acquisition's epoch stamp: the value owned appends carry in their
    /// frame fence field. Reading it is inspection only.
    pub fn epoch(&self) -> u64 {
        self.held_tenure().epoch()
    }

    /// The session this writer was opened for.
    pub fn session(&self) -> &BindIdentity {
        self.held_tenure().session()
    }

    /// The lease root the tenure was acquired under. Carried, not checked:
    /// matching it against the store's configured domain is the store
    /// opener's job when it adopts this facade.
    pub fn lease_root(&self) -> &Path {
        self.held_tenure().lease_root()
    }

    fn held_tenure(&self) -> &OwnedWalTenure {
        self.wal
            .owned_tenure()
            .expect("an owned writer holds its tenure until the consuming close")
    }

    /// Settle queued work under the still-held tenure, disable the writer,
    /// and hand back the held tenure. The explicit order is the guarantee:
    /// the final barrier runs first, the mode flips to `Closed` (refusing any
    /// later mutation), the descriptor drops, and only then does the tenure
    /// move out. Dropping the facade without closing skips the final barrier,
    /// so normal shutdown must call this; like everywhere else, nothing is
    /// written or acknowledged from `Drop`.
    pub fn close_into_tenure(self) -> OwnedCloseOutcome {
        let mut wal = self.wal;
        let sync_result = wal.fsync();
        #[cfg(test)]
        if sync_result.is_ok() {
            OWNED_CLOSE_SETTLES.with(|count| count.set(count.get() + 1));
        }
        let tenure = wal
            .take_owned_tenure()
            .expect("an owned writer holds its tenure until the consuming close");
        drop(wal);
        OwnedCloseOutcome {
            sync_result,
            tenure,
        }
    }
}

/// Read all bytes + decode records, discarding a torn tail, failing loud on
/// non-tail corruption + seq regression.
fn replay_path(path: &Path, max_bytes: u64) -> Result<Vec<FramedRecord>, WalError> {
    Ok(replay_path_with_valid_end(path, max_bytes)?.0)
}

/// Read only the bounded prefix through the first RunStarted. Sequence-bounded range
/// reads still load full files, so admission/status classification needs this byte-bounded
/// reader instead. It never opens a writer or repairs a torn tail.
pub fn read_first_run_started(
    path: impl AsRef<Path>,
    max_bytes: u64,
) -> Result<Option<FramedRecord>, WalError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WalError::Io(error)),
    };
    read_first_run_started_from(file, max_bytes)
}

fn read_first_run_started_from(
    file: impl Read,
    max_bytes: u64,
) -> Result<Option<FramedRecord>, WalError> {
    read_prefix_until_from(file, max_bytes, |framed| {
        if matches!(framed.record, WalRecord::RunStarted { .. }) {
            return Some(framed.clone());
        }
        None
    })
    .map(|(found, _)| found)
}

/// The session's first durable user message: the first `Role::User` message in the first
/// `RunStarted` or `Steered` record that carries one, read only as far as that record. Returns
/// it with the number of bytes read; `None` when the log holds none (or is absent). Like
/// [`read_first_run_started`], a read that would pass `max_bytes` fails with
/// [`WalError::TooLarge`] instead of loading the rest of the log, and a torn tail ends the read.
pub fn read_first_user_message(
    path: impl AsRef<Path>,
    max_bytes: u64,
) -> Result<(Option<broca_protocol::Message>, u64), WalError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((None, 0)),
        Err(error) => return Err(WalError::Io(error)),
    };
    read_prefix_until_from(file, max_bytes, |framed| {
        first_user_message_in(&framed.record)
    })
}

/// The first user message a record adds to the conversation, if it adds one.
pub fn first_user_message_in(record: &WalRecord) -> Option<broca_protocol::Message> {
    let input = match record {
        WalRecord::RunStarted { input, .. } | WalRecord::Steered { input, .. } => input,
        _ => return None,
    };
    input
        .iter()
        .find(|message| message.role == broca_protocol::Role::User)
        .cloned()
}

/// Decode only as far as the requested value. Damage is classified by the same
/// decoder as full replay, with lookahead bounded by the caller's byte budget.
fn read_prefix_until_from<T>(
    file: impl Read,
    max_bytes: u64,
    found: impl FnMut(&FramedRecord) -> Option<T>,
) -> Result<(Option<T>, u64), WalError> {
    let scan = scan_prefix(file, max_bytes, found)?;
    Ok((scan.found, scan.bytes_read))
}

struct PrefixScan<T> {
    found: Option<T>,
    bytes_read: u64,
    valid_end: u64,
    last_seq: Option<u64>,
    lineage_id: Option<WalLineageId>,
}

fn read_up_to(file: &mut impl Read, bytes: &mut [u8]) -> Result<usize, WalError> {
    let mut n = 0;
    while n < bytes.len() {
        match file.read(&mut bytes[n..]) {
            Ok(0) => break,
            Ok(read) => n += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(WalError::Io(error)),
        }
    }
    Ok(n)
}

/// Strictly decode one body at a time, retaining only the caller's selected value.
/// Recovery needs the verified end and sequence, not all historical message bodies.
fn scan_prefix<T>(
    mut file: impl Read,
    max_bytes: u64,
    mut found: impl FnMut(&FramedRecord) -> Option<T>,
) -> Result<PrefixScan<T>, WalError> {
    let mut scan = PrefixScan {
        found: None,
        bytes_read: 0,
        valid_end: 0,
        last_seq: None,
        lineage_id: None,
    };
    loop {
        let offset = scan.valid_end;
        let mut header = [0; crate::framing::HEADER_LEN];
        let available = max_bytes.saturating_sub(offset);
        let header_budget = available.min(header.len() as u64) as usize;
        let n = read_up_to(&mut file, &mut header[..header_budget])?;
        scan.bytes_read += n as u64;
        if n == 0 && header_budget != 0 {
            return Ok(scan);
        }
        let mut frame = header[..n].to_vec();
        if n == header.len() {
            let len = u32::from_le_bytes(header[..4].try_into().unwrap());
            // A version 4 header is longer than the legacy one: its requirements array comes
            // before the CRC and the payload. Read that array before sizing the frame from
            // `len`, or the reader would assemble a frame short of its own header and never
            // reach the payload, which a crash leaves unwritten, so recovery discards it.
            let header_len = if header[4] == crate::framing::V4_FRAMING_VERSION {
                match crate::framing::v4_header_len(&header) {
                    Some(header_len) => header_len,
                    None => return Ok(scan),
                }
            } else {
                header.len()
            };
            if header_len > header.len() {
                let extra = header_len - header.len();
                let extra_budget = max_bytes.saturating_sub(offset + header.len() as u64);
                let read = extra.min(usize::try_from(extra_budget).unwrap_or(usize::MAX));
                let mut rest = vec![0u8; read];
                let n = read_up_to(&mut file, &mut rest)?;
                scan.bytes_read += n as u64;
                rest.truncate(n);
                frame.extend_from_slice(&rest);
            }
            if len <= crate::framing::MAX_PAYLOAD_LEN {
                let size = header_len + len as usize;
                // The declared payload may be a torn tail's damaged length. Read
                // only within budget; exceptional classification below probes one
                // more byte to distinguish EOF from an actually oversized source.
                let bounded_size = size.min(usize::try_from(available).unwrap_or(usize::MAX));
                let already = frame.len();
                frame.resize(bounded_size.max(already), 0);
                let n = read_up_to(&mut file, &mut frame[already..])?;
                frame.truncate(already + n);
                scan.bytes_read += n as u64;
            }
        }
        match decode_next_counted(&frame) {
            DecodeStep::Record { framed, consumed } => {
                validate_contiguous_sequence(scan.last_seq, 1, framed.seq)?;
                scan.last_seq = Some(framed.seq);
                scan.valid_end += consumed as u64;
                if let Some(value) = found(&framed) {
                    scan.found = Some(value);
                    return Ok(scan);
                }
            }
            DecodeStep::Lineage {
                lineage_id,
                consumed,
            } => {
                fold_lineage_id(&mut scan.lineage_id, lineage_id)?;
                scan.valid_end += consumed as u64;
            }
            DecodeStep::NewerWriter { ver, missing } => {
                return Err(WalError::NewerWriter {
                    offset: offset as usize,
                    ver,
                    missing,
                });
            }
            DecodeStep::Corrupt(_) | DecodeStep::TornTail => {
                // Only the exceptional path reads ahead. A damaged length cannot tell
                // us whether intact records follow it, so inspect the bounded suffix.
                let before = frame.len();
                file.take(available.saturating_sub(before as u64).saturating_add(1))
                    .read_to_end(&mut frame)
                    .map_err(WalError::Io)?;
                scan.bytes_read += (frame.len() - before) as u64;
                if frame.len() as u64 > available {
                    return Err(WalError::TooLarge {
                        max_bytes,
                        file_size: offset + frame.len() as u64,
                    });
                }
                decode_bytes_seeded(
                    &frame,
                    offset,
                    scan.last_seq,
                    1,
                    Vec::new(),
                    scan.lineage_id,
                )?;
                return Ok(scan);
            }
        }
    }
}

/// READ-ONLY replay of a WAL file: open read-only, decode the committed records,
/// discard a torn tail IN MEMORY. Unlike [`FileWal::open`], this NEVER opens a write
/// handle and NEVER truncates the file — so it is safe to call CONCURRENTLY with the
/// single live writer (a reader must never mutate the writer's file). A concurrent
/// in-progress append is seen as a torn tail and ignored for this read; the writer's
/// own bytes are untouched. This is the reader entry point (e.g. a subscriber's attach
/// replay); the writer-recovery truncation belongs only to `open`.
pub fn replay_file(path: impl AsRef<Path>, max_bytes: u64) -> Result<Vec<FramedRecord>, WalError> {
    replay_path(path.as_ref(), max_bytes)
}

/// Whether a WAL file EXISTS, separately from whether it holds anything.
///
/// `replay_file` cannot answer this and must not try. It returns an empty vector for an absent
/// file AND for a present empty one (`read_file_capped` maps `NotFound` to `Ok(Vec::new())`,
/// and a zero-byte read produces the same empty buffer), so those two states share a return
/// value at every call site above it.
///
/// The conflation is CORRECT for resume, which only asks what has been committed. It is wrong
/// for any caller deciding whether a session's log is MISSING, because:
///
///   - an absent WAL under a store row that claims content is an anomaly worth reporting;
///   - a present empty WAL is an ordinary lineage that was opened and never ran (634 of them
///     on the production tree at the time of writing);
///   - and after a WAL archive lands, "absent from the direct path" becomes the ROUTINE case
///     for every cold session, at which point absence stops being evidence of anything.
///
/// That last point is why this exists BEFORE the archive rather than alongside it: the archive
/// consumes the signal, so the distinction has to be drawn while it still means something.
///
/// Returns `Err` only for an IO failure that is neither presence nor absence — a permission
/// error is not evidence either way, and answering `false` for it would be the same defect
/// this function exists to remove.
pub fn wal_file_exists(path: impl AsRef<Path>) -> Result<bool, WalError> {
    match fs_ops::metadata(path.as_ref()) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(WalError::Io(e)),
    }
}

/// READ-ONLY range replay that succeeds only when `anchor` is valid for the current file.
///
/// Unlike [`read_records_range`], this never falls back to decoding from byte zero. It also
/// rejects gaps inside the selected range so every current or future strict-anchor caller gets an
/// exactly contiguous suffix. Callers use `None` to abandon the optimization and take their
/// existing full-replay safety path.
pub fn read_records_range_from_valid_anchor(
    path: impl AsRef<Path>,
    max_bytes: u64,
    after_seq: u64,
    through_seq: Option<u64>,
    anchor: &WalByteAnchor,
) -> Result<Option<WalRangeRead>, WalError> {
    let path = path.as_ref();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WalError::Io(error)),
    };
    let metadata = file.metadata().map_err(WalError::Io)?;
    let file_size = metadata.len();
    if file_size > max_bytes {
        return Err(WalError::TooLarge {
            max_bytes,
            file_size,
        });
    }
    let (Some(file_id), Some(expected_next)) =
        (metadata_file_id(&metadata), after_seq.checked_add(1))
    else {
        return Ok(None);
    };
    if anchor.file_id != file_id
        || anchor.seq != after_seq
        || anchor.byte_end >= file_size
        || anchor.byte_end > max_bytes
    {
        return Ok(None);
    }

    let suffix = read_open_file_capped(&mut file, anchor.byte_end, max_bytes, file_size)?;
    let Some(decoded) = decode_anchored_suffix(&suffix, anchor.byte_end, expected_next)? else {
        return Ok(None);
    };
    let range = select_decoded_range(
        decoded,
        &suffix,
        anchor.byte_end,
        Some(file_id),
        after_seq,
        through_seq,
    )?;
    if range
        .records
        .windows(2)
        .any(|pair| pair[0].seq.checked_add(1) != Some(pair[1].seq))
    {
        return Ok(None);
    }
    Ok(Some(range))
}

/// READ-ONLY range replay of a WAL file.
///
/// Returns records with `seq > after_seq`, additionally excluding records past
/// `through_seq` when supplied. The upper bound is intended for a caller's last fsynced
/// safe head, because a read-only handle can observe newer flushed bytes.
///
/// A matching anchor is used only when it names `after_seq` and the frame immediately at
/// `byte_end` decodes cleanly as `after_seq + 1`. A changed file, shrink, bad offset, or
/// unexpected frame silently falls back to decoding from byte zero. Correctness therefore
/// never depends on persisted anchor metadata.
///
/// Like [`replay_file`], this opens no write handle and discards a torn tail in memory.
pub fn read_records_range(
    path: impl AsRef<Path>,
    max_bytes: u64,
    after_seq: u64,
    through_seq: Option<u64>,
    anchor: Option<&WalByteAnchor>,
) -> Result<WalRangeRead, WalError> {
    let path = path.as_ref();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WalRangeRead {
                records: Vec::new(),
                next_anchor: None,
                lineage_id: None,
                file_present: Some(false),
                complete_byte_end: None,
                archive_lookup_missed: false,
                anchors: Vec::new(),
            });
        }
        Err(error) => return Err(WalError::Io(error)),
    };
    let metadata = file.metadata().map_err(WalError::Io)?;
    let file_size = metadata.len();
    if file_size > max_bytes {
        return Err(WalError::TooLarge {
            max_bytes,
            file_size,
        });
    }
    let file_id = metadata_file_id(&metadata);

    if let (Some(anchor), Some(file_id), Some(expected_next)) =
        (anchor, file_id, after_seq.checked_add(1))
    {
        let identity_matches = anchor.file_id == file_id && anchor.seq == after_seq;
        if identity_matches && anchor.byte_end < file_size && anchor.byte_end <= max_bytes {
            let suffix = read_open_file_capped(&mut file, anchor.byte_end, max_bytes, file_size)?;
            if let Some(decoded) = decode_anchored_suffix(&suffix, anchor.byte_end, expected_next)?
            {
                return select_decoded_range(
                    decoded,
                    &suffix,
                    anchor.byte_end,
                    Some(file_id),
                    after_seq,
                    through_seq,
                );
            }
        }
    }

    let bytes = read_open_file_capped(&mut file, 0, max_bytes, file_size)?;
    let decoded = decode_bytes(&bytes, 0, 1)?;
    select_decoded_range(decoded, &bytes, 0, file_id, after_seq, through_seq)
}

/// The newest records of a WAL file, read from at most its last `tail_bytes`.
pub struct WalTailRead {
    /// Contiguous records ending at the file's last complete record. A torn trailing append
    /// is discarded, as every reader does.
    pub records: Vec<FramedRecord>,
    /// The read began at the file's first byte, so `records` holds the whole WAL.
    pub from_start: bool,
    /// The file's lineage identity, when its first frame or the bytes read carry one.
    pub lineage_id: Option<WalLineageId>,
    /// Whether the file existed.
    pub file_present: bool,
}

/// Read the newest records of a WAL without decoding it from the start.
///
/// Frames carry no back-pointers, so the read finds the first frame boundary inside the
/// last `tail_bytes` by trying each offset: an offset counts only when a frame there verifies
/// its digest and every following frame decodes in sequence to the end of the file. JSON
/// payloads cannot hold a frame's raw binary header, so a boundary found this way is a real
/// one. When no boundary is found (a single frame larger than the window) the whole file is
/// read instead. Like [`read_records_range`], this opens no write handle.
pub fn read_records_tail(
    path: impl AsRef<Path>,
    max_bytes: u64,
    tail_bytes: u64,
) -> Result<WalTailRead, WalError> {
    let path = path.as_ref();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WalTailRead {
                records: Vec::new(),
                from_start: true,
                lineage_id: None,
                file_present: false,
            });
        }
        Err(error) => return Err(WalError::Io(error)),
    };
    let file_size = file.metadata().map_err(WalError::Io)?.len();
    if file_size > max_bytes {
        return Err(WalError::TooLarge {
            max_bytes,
            file_size,
        });
    }
    if tail_bytes < file_size {
        let start = file_size - tail_bytes;
        let suffix = read_open_file_capped(&mut file, start, max_bytes, file_size)?;
        let first_frame = read_first_frame_lineage(&mut file, max_bytes)?;
        for offset in 0..suffix.len() {
            let DecodeStep::Record { framed, .. } = decode_next(&suffix[offset..]) else {
                continue;
            };
            if let Ok(decoded) = decode_bytes(&suffix[offset..], start + offset as u64, framed.seq)
            {
                return Ok(WalTailRead {
                    records: decoded
                        .records
                        .into_iter()
                        .map(|(record, _)| record)
                        .collect(),
                    from_start: false,
                    lineage_id: decoded.lineage_id.or(first_frame),
                    file_present: true,
                });
            }
        }
    }
    let bytes = read_open_file_capped(&mut file, 0, max_bytes, file_size)?;
    let decoded = decode_bytes(&bytes, 0, 1)?;
    Ok(WalTailRead {
        records: decoded
            .records
            .into_iter()
            .map(|(record, _)| record)
            .collect(),
        from_start: true,
        lineage_id: decoded.lineage_id,
        file_present: true,
    })
}

/// Decode at most the final `tail_bytes` of a live WAL. A frame larger than the
/// window or an undecodable suffix returns `None`, never a full-history fallback.
pub fn read_records_tail_bounded(
    path: impl AsRef<Path>,
    max_bytes: u64,
    tail_bytes: u64,
) -> Result<Option<WalRangeRead>, WalError> {
    let path = path.as_ref();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WalError::Io(error)),
    };
    let metadata = file.metadata().map_err(WalError::Io)?;
    read_records_tail_snapshot(&mut file, &metadata, max_bytes, tail_bytes)
}

fn read_records_tail_snapshot(
    file: &mut File,
    metadata: &std::fs::Metadata,
    max_bytes: u64,
    tail_bytes: u64,
) -> Result<Option<WalRangeRead>, WalError> {
    let file_size = metadata.len();
    if file_size > max_bytes {
        return Err(WalError::TooLarge {
            max_bytes,
            file_size,
        });
    }
    let start = file_size.saturating_sub(tail_bytes);
    // The observed length bounds a snapshot, not the safety cap. A capped EOF
    // probe would mistake a concurrent append for an over-cap file. Read only
    // the already size-checked window, keeping both the cap and tail bound intact.
    file.seek(SeekFrom::Start(start)).map_err(WalError::Io)?;
    let mut suffix = Vec::new();
    file.take(file_size - start)
        .read_to_end(&mut suffix)
        .map_err(WalError::Io)?;
    let lineage = read_first_frame_lineage(file, max_bytes)?;
    let file_id = metadata_file_id(metadata);
    if start == 0 {
        let decoded = decode_bytes(&suffix, 0, 1)?;
        return select_decoded_range(decoded, &suffix, 0, file_id, 0, None).map(Some);
    }
    for offset in 0..suffix.len() {
        let DecodeStep::Record { framed, .. } = decode_next(&suffix[offset..]) else {
            continue;
        };
        let after = framed.seq.saturating_sub(1);
        if let Ok(decoded) = decode_bytes(&suffix[offset..], start + offset as u64, framed.seq) {
            let mut range = select_decoded_range(
                decoded,
                &suffix[offset..],
                start + offset as u64,
                file_id,
                after,
                None,
            )?;
            range.lineage_id = range.lineage_id.or(lineage);
            return Ok(Some(range));
        }
    }
    Ok(None)
}

/// The lineage identity a WAL records in its first frame. A WAL that predates lineage
/// identities starts with a record instead, and answers `None`.
fn read_first_frame_lineage(
    file: &mut File,
    max_bytes: u64,
) -> Result<Option<WalLineageId>, WalError> {
    file.seek(SeekFrom::Start(0)).map_err(WalError::Io)?;
    let mut head = Vec::new();
    file.take(4096.min(max_bytes))
        .read_to_end(&mut head)
        .map_err(WalError::Io)?;
    WAL_BYTES_READ.fetch_add(head.len() as u64, std::sync::atomic::Ordering::Relaxed);
    Ok(match decode_next(&head) {
        DecodeStep::Lineage { lineage_id, .. } => Some(lineage_id),
        _ => None,
    })
}

/// Decode a digest-verified archive member in place, preserving WAL lineage and sequence
/// validation. The caller supplies an identity for the immutable payload, not a live inode.
/// Archive corruption must be an error before this call, never a `Partial` transcript:
/// consumers currently treat Partial alongside Complete and could silently serve a prefix.
pub fn read_verified_member_range(
    bytes: &[u8],
    member_identity: u64,
    after_seq: u64,
    through_seq: Option<u64>,
) -> Result<WalRangeRead, WalError> {
    #[cfg(any(test, feature = "test-io-trace"))]
    crate::archive::expiry_io::decoded();
    let decoded = decode_bytes(bytes, 0, 1)?;
    select_decoded_range(
        decoded,
        bytes,
        0,
        Some(member_identity),
        after_seq,
        through_seq,
    )
}

struct DecodedBytes {
    records: Vec<(FramedRecord, u64)>,
    valid_end: u64,
    lineage_id: Option<WalLineageId>,
}

#[cfg(test)]
std::thread_local! {
    static DECODED_BYTE_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn decode_next_counted(bytes: &[u8]) -> DecodeStep {
    let step = decode_next(bytes);
    #[cfg(test)]
    if let DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } = &step {
        DECODED_BYTE_COUNT.with(|count| count.set(count.get() + *consumed as u64));
    }
    step
}

/// Fold a decoded lineage frame into the identity observed so far, REFUSING a conflict.
///
/// Single-sited because it is an INVARIANT, not a convenience. Two decoders read the same byte
/// stream -- `decode_anchored_suffix` behind the anchored range reads that `session.read` uses,
/// and `decode_bytes_seeded` behind `replay_file` -- and a WAL carrying two different lineage
/// identities is a spliced or overwritten file whose records cannot be attributed to a session.
///
/// It was duplicated verbatim in both until 2026-09-02, which is the shape that matters: if one
/// copy lost the check during a refactor the same file would fail loud through `replay_file` and
/// decode CLEANLY through an anchored read. The silent arm would be the hot one, and the
/// divergence would surface as a consumer seeing an identity that quietly changed rather than as
/// an error anyone could trace back here.
fn fold_lineage_id(
    observed: &mut Option<WalLineageId>,
    found: WalLineageId,
) -> Result<(), WalError> {
    match *observed {
        Some(first) if first != found => Err(WalError::ConflictingLineageIds { first, found }),
        Some(_) => Ok(()),
        None => {
            *observed = Some(found);
            Ok(())
        }
    }
}

fn decode_anchored_suffix(
    bytes: &[u8],
    absolute_start: u64,
    expected_next: u64,
) -> Result<Option<DecodedBytes>, WalError> {
    let mut offset = 0usize;
    let mut lineage_id = None;
    while offset < bytes.len() {
        open_observed("frame decode");
        match decode_next_counted(&bytes[offset..]) {
            DecodeStep::Lineage {
                lineage_id: found,
                consumed,
            } => {
                fold_lineage_id(&mut lineage_id, found)?;
                offset += consumed;
            }
            DecodeStep::Record { framed, consumed } => {
                if let Err(error) = validate_contiguous_sequence(None, expected_next, framed.seq) {
                    return match error {
                        WalError::SequenceGap { .. } => Ok(None),
                        error => Err(error),
                    };
                }
                let first_seq = framed.seq;
                let first_end = absolute_start + offset as u64 + consumed as u64;
                let records = vec![(*framed, first_end)];
                return match decode_bytes_seeded(
                    &bytes[offset + consumed..],
                    first_end,
                    Some(first_seq),
                    expected_next,
                    records,
                    lineage_id,
                ) {
                    Ok(decoded) => Ok(Some(decoded)),
                    // Return `None` for a gap in an anchored suffix so the caller retries from byte
                    // zero. The full replay then reports the missing sequence as corruption.
                    Err(WalError::SequenceGap { .. }) => Ok(None),
                    Err(error) => Err(error),
                };
            }
            DecodeStep::TornTail
                if offset > 0 && !has_intact_data_after(&bytes[offset..], lineage_id) =>
            {
                return Ok(Some(DecodedBytes {
                    records: Vec::new(),
                    valid_end: absolute_start + offset as u64,
                    lineage_id,
                }));
            }
            // A complete frame that only a newer build can read is an answer, not a reason to
            // retry from byte zero. A corrupt frame here may just mean the stored anchor points
            // at the wrong byte offset, so the caller retries from zero; this frame's digest
            // verified, so the offset is a real frame boundary, and a full read would only
            // decode the whole prefix to reach the same refusal.
            DecodeStep::NewerWriter { ver, missing } => {
                return Err(WalError::NewerWriter {
                    offset: (absolute_start + offset as u64) as usize,
                    ver,
                    missing,
                });
            }
            DecodeStep::TornTail | DecodeStep::Corrupt(_) => return Ok(None),
        }
    }
    Ok(Some(DecodedBytes {
        records: Vec::new(),
        valid_end: absolute_start + offset as u64,
        lineage_id,
    }))
}

fn decode_bytes(
    bytes: &[u8],
    absolute_start: u64,
    expected_first_seq: u64,
) -> Result<DecodedBytes, WalError> {
    decode_bytes_seeded(
        bytes,
        absolute_start,
        None,
        expected_first_seq,
        Vec::new(),
        None,
    )
}

fn validate_contiguous_sequence(
    prev_seq: Option<u64>,
    expected_first_seq: u64,
    seq: u64,
) -> Result<(), WalError> {
    let expected_seq = match prev_seq {
        Some(prev_seq) => {
            if seq <= prev_seq {
                return Err(WalError::NonMonotonic { prev_seq, seq });
            }
            prev_seq
                .checked_add(1)
                .ok_or(WalError::NonMonotonic { prev_seq, seq })?
        }
        None => expected_first_seq,
    };
    if seq != expected_seq {
        return Err(WalError::SequenceGap { expected_seq, seq });
    }
    Ok(())
}

// A damaged length cannot identify the next boundary. Search for intact frames
// instead; matching lineage-only metadata after a damaged last record is part
// of its uncommitted tail. Unreadable frames and conflicting identities are not.
fn has_intact_data_after(bytes: &[u8], mut expected_lineage: Option<WalLineageId>) -> bool {
    (1..bytes.len().saturating_sub(crate::framing::HEADER_LEN - 1)).any(|offset| {
        let suffix = &bytes[offset..];
        match crate::framing::decode_next(suffix) {
            DecodeStep::Record { .. } | DecodeStep::NewerWriter { .. } => true,
            DecodeStep::Corrupt(_) => crate::framing::digest_verifies(suffix),
            DecodeStep::Lineage {
                lineage_id: found, ..
            } => {
                if expected_lineage.is_some_and(|first| first != found) {
                    true
                } else {
                    expected_lineage = Some(found);
                    false
                }
            }
            DecodeStep::TornTail => false,
        }
    })
}

fn decode_bytes_seeded(
    bytes: &[u8],
    absolute_start: u64,
    mut prev_seq: Option<u64>,
    expected_first_seq: u64,
    mut records: Vec<(FramedRecord, u64)>,
    mut lineage_id: Option<WalLineageId>,
) -> Result<DecodedBytes, WalError> {
    let mut offset = 0usize;
    while offset < bytes.len() {
        open_observed("frame decode");
        match decode_next_counted(&bytes[offset..]) {
            DecodeStep::Record { framed, consumed } => {
                validate_contiguous_sequence(prev_seq, expected_first_seq, framed.seq)?;
                prev_seq = Some(framed.seq);
                offset += consumed;
                records.push((*framed, absolute_start + offset as u64));
            }
            DecodeStep::Lineage {
                lineage_id: found,
                consumed,
            } => {
                fold_lineage_id(&mut lineage_id, found)?;
                offset += consumed;
            }
            DecodeStep::TornTail if has_intact_data_after(&bytes[offset..], lineage_id) => {
                return Err(WalError::Corrupt {
                    offset: (absolute_start + offset as u64) as usize,
                    source: FrameError::NonTailIncompleteFrame,
                });
            }
            DecodeStep::TornTail => break,
            DecodeStep::Corrupt(source) => {
                if !crate::framing::digest_verifies(&bytes[offset..])
                    && !has_intact_data_after(&bytes[offset..], lineage_id)
                {
                    break;
                }
                return Err(WalError::Corrupt {
                    offset: (absolute_start + offset as u64) as usize,
                    source,
                });
            }
            DecodeStep::NewerWriter { ver, missing } => {
                return Err(WalError::NewerWriter {
                    offset: (absolute_start + offset as u64) as usize,
                    ver,
                    missing,
                });
            }
        }
    }
    Ok(DecodedBytes {
        records,
        valid_end: absolute_start + offset as u64,
        lineage_id,
    })
}

fn select_decoded_range(
    decoded: DecodedBytes,
    _bytes: &[u8],
    _absolute_start: u64,
    file_id: Option<u64>,
    after_seq: u64,
    through_seq: Option<u64>,
) -> Result<WalRangeRead, WalError> {
    let source_head = decoded.records.last().map(|(r, _)| r.seq);
    let complete_end = decoded.valid_end;
    let mut range = select_range(
        decoded.records,
        file_id,
        after_seq,
        through_seq,
        decoded.lineage_id,
    );
    if source_head.is_some() && range.next_anchor.map(|a| a.seq) == source_head {
        range.complete_byte_end = Some(complete_end);
    }
    Ok(range)
}

fn select_range(
    decoded: Vec<(FramedRecord, u64)>,
    file_id: Option<u64>,
    after_seq: u64,
    through_seq: Option<u64>,
    lineage_id: Option<WalLineageId>,
) -> WalRangeRead {
    let mut records = Vec::new();
    let mut anchors = Vec::new();
    for (record, byte_end) in decoded {
        if record.seq > after_seq && through_seq.is_none_or(|head| record.seq <= head) {
            if let Some(file_id) = file_id {
                anchors.push(WalByteAnchor {
                    file_id,
                    seq: record.seq,
                    byte_end,
                });
            }
            records.push(record);
        }
    }
    let mut range = WalRangeRead::with_anchors_and_lineage(records, anchors, lineage_id);
    // Callers either opened a live file or decoded a verified archive member. Both
    // observed a complete source; test-double constructors deliberately leave it unknown.
    range.file_present = Some(true);
    range
}

/// Bytes this process has read from WAL files through the range and tail readers.
static WAL_BYTES_READ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Bytes this process has read from WAL files through the range and tail readers, for
/// measuring what a reader costs; archive member reads are not included.
pub fn wal_bytes_read() -> u64 {
    WAL_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed)
}

fn read_open_file_capped(
    file: &mut File,
    start: u64,
    max_bytes: u64,
    observed_size: u64,
) -> Result<Vec<u8>, WalError> {
    file.seek(SeekFrom::Start(start)).map_err(WalError::Io)?;
    let mut bytes = Vec::new();
    let read_cap = max_bytes.saturating_sub(start).saturating_add(1);
    file.take(read_cap)
        .read_to_end(&mut bytes)
        .map_err(WalError::Io)?;
    WAL_BYTES_READ.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
    if start.saturating_add(bytes.len() as u64) > max_bytes {
        return Err(WalError::TooLarge {
            max_bytes,
            file_size: observed_size.max(start.saturating_add(bytes.len() as u64)),
        });
    }
    Ok(bytes)
}

#[cfg(unix)]
fn metadata_file_id(metadata: &std::fs::Metadata) -> Option<u64> {
    use std::{hash::Hash, os::unix::fs::MetadataExt};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    metadata.dev().hash(&mut hasher);
    metadata.ino().hash(&mut hasher);
    Some(std::hash::Hasher::finish(&hasher))
}

#[cfg(not(unix))]
fn metadata_file_id(_metadata: &std::fs::Metadata) -> Option<u64> {
    // The standard library does not expose a stable Windows file identifier. Return no identity
    // rather than cache against a weak timestamp surrogate: no identity simply makes each idle
    // read take its flush-only proof, which under-optimizes but never over-claims durability.
    None
}

/// Return the platform-stable identity of the file currently at `path`, when the platform can
/// provide one. Read-side durability caches use this only to invalidate a proof after the WAL is
/// replaced; callers without an identity must take a fresh flush barrier instead.
pub fn file_identity(path: impl AsRef<Path>) -> Result<Option<u64>, WalError> {
    let metadata = match fs_ops::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WalError::Io(error)),
    };
    Ok(metadata_file_id(&metadata))
}

/// Like [`replay_path`] but also returns the byte offset where valid frames end (the start of
/// any torn tail, or the whole file length if intact). `open()` uses this offset
/// to truncate a torn tail so the next append never writes past it.
fn replay_path_with_valid_end(
    path: &Path,
    max_bytes: u64,
) -> Result<(Vec<FramedRecord>, u64, Option<WalLineageId>), WalError> {
    let buf = read_file_capped(path, max_bytes)?;
    let decoded = decode_bytes(&buf, 0, 1)?;
    Ok((
        decoded
            .records
            .into_iter()
            .map(|(record, _)| record)
            .collect(),
        decoded.valid_end,
        decoded.lineage_id,
    ))
}

fn read_file_capped(path: &Path, max_bytes: u64) -> Result<Vec<u8>, WalError> {
    open_observed("read");
    match File::open(path) {
        Ok(f) => {
            let file_size = f.metadata().map_err(WalError::Io)?.len();
            if file_size > max_bytes {
                return Err(WalError::TooLarge {
                    max_bytes,
                    file_size,
                });
            }
            let mut buf = Vec::with_capacity(file_size as usize);
            let mut limited = f.take(max_bytes.saturating_add(1));
            limited.read_to_end(&mut buf).map_err(WalError::Io)?;
            if buf.len() as u64 > max_bytes {
                return Err(WalError::TooLarge {
                    max_bytes,
                    file_size: buf.len() as u64,
                });
            }
            Ok(buf)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(WalError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn range_reads_do_not_restore_unused_feature_collection_api_or_pass() {
        let reader = include_str!("file_wal.rs");
        let framing = include_str!("framing.rs");
        for obsolete in [
            concat!("pub fn used_", "features("),
            concat!("used_", "features: Vec<String>"),
        ] {
            assert!(
                !reader.contains(obsolete),
                "range reads must not retain unused feature collection: {obsolete}"
            );
        }
        assert!(
            !framing.contains(concat!("fn verified_prefix_", "features(")),
            "feature checks belong to the decoder, not a second pass over verified records"
        );
    }

    #[test]
    fn near_cap_torn_headers_recover_before_budget_refusal() {
        fn open_with_cap(path: &Path, cap: u64) -> Result<FileWal, WalError> {
            struct RestoreCap(u64);
            impl Drop for RestoreCap {
                fn drop(&mut self) {
                    OPEN_READ_CAP.with(|cap| cap.set(self.0));
                }
            }
            let _restore = RestoreCap(OPEN_READ_CAP.with(|limit| limit.replace(cap)));
            FileWal::open(path)
        }
        fn verified_prefix(tag: &str) -> (PathBuf, Vec<u8>) {
            let path = tmp(tag);
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(7, &WalRecord::StepStarted { step_id: StepId(1) }, true)
                .unwrap();
            drop(wal);
            let prefix = std::fs::read(&path).unwrap();
            (path, prefix)
        }
        let next = WalRecord::StepStarted { step_id: StepId(2) };
        let intact_frame = encode(2, 7, &next).unwrap();
        let mut header = intact_frame[..crate::framing::HEADER_LEN].to_vec();
        header[..4].copy_from_slice(&4096u32.to_le_bytes());
        for slack in [0, 7] {
            let (path, prefix) = verified_prefix(&format!("near-cap-torn-{slack}"));
            let mut damaged = prefix.clone();
            damaged.extend_from_slice(&header);
            std::fs::write(&path, &damaged).unwrap();
            let cap = damaged.len() as u64 + slack;
            assert_eq!(replay_file(&path, cap).unwrap().len(), 1);
            assert_eq!(
                std::fs::read(&path).unwrap(),
                damaged,
                "readers never trim the tail"
            );
            let mut recovered = open_with_cap(&path, cap)
                .expect("EOF within the cap is a recoverable torn tail, not an oversized WAL");
            assert_eq!(recovered.last_seq(), 1);
            assert_eq!(std::fs::read(&path).unwrap(), prefix);
            assert_eq!(recovered.append(7, &next, true).unwrap(), 2);
            drop(recovered);
            assert_eq!(FileWal::open(&path).unwrap().replay().unwrap().len(), 2);
        }

        let (path, prefix) = verified_prefix("near-cap-intact-control");
        let mut intact = prefix.clone();
        intact.extend_from_slice(&intact_frame);
        std::fs::write(&path, &intact).unwrap();
        let cap = prefix.len() as u64 + crate::framing::HEADER_LEN as u64 + 1;
        assert!(matches!(
            scan_prefix(std::io::Cursor::new(&intact), cap, |_| None::<()>),
            Err(WalError::TooLarge { .. })
        ));
        assert!(matches!(
            open_with_cap(&path, cap),
            Err(WalError::TooLarge { .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), intact);

        let mut non_tail = prefix;
        non_tail.extend_from_slice(&header);
        non_tail.extend_from_slice(&intact_frame);
        std::fs::write(&path, &non_tail).unwrap();
        assert!(matches!(
            open_with_cap(&path, non_tail.len() as u64),
            Err(WalError::Corrupt {
                source: FrameError::NonTailIncompleteFrame,
                ..
            })
        ));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            non_tail,
            "intact later data forbids tail repair"
        );
    }

    #[test]
    fn prefix_readers_share_replay_damage_policy_without_writing() {
        let first = encode(1, 1, &WalRecord::StepStarted { step_id: StepId(1) }).unwrap();
        let second = encode(2, 1, &WalRecord::StepStarted { step_id: StepId(2) }).unwrap();
        for (tag, bytes, corrupt) in [
            (
                "bad-final-digest",
                {
                    let mut bytes = first.clone();
                    *bytes.last_mut().unwrap() ^= 1;
                    bytes
                },
                false,
            ),
            (
                "damaged-nontail-length",
                {
                    let mut bytes = first.clone();
                    bytes[..4].copy_from_slice(&4096u32.to_le_bytes());
                    bytes.extend_from_slice(&second);
                    bytes
                },
                true,
            ),
            (
                "incomplete-final-frame",
                first[..first.len() - 1].to_vec(),
                false,
            ),
            (
                "bad-nontail-digest",
                {
                    let mut bytes = first.clone();
                    *bytes.last_mut().unwrap() ^= 1;
                    bytes.extend_from_slice(&second);
                    bytes
                },
                true,
            ),
        ] {
            let path = tmp(tag);
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(replay_file(&path, 8192).is_err(), corrupt, "{tag}");
            assert_eq!(
                read_first_run_started(&path, 8192).is_err(),
                corrupt,
                "{tag}"
            );
            assert_eq!(
                read_first_user_message(&path, 8192).is_err(),
                corrupt,
                "{tag}"
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                bytes,
                "readers must not repair {tag}"
            );
        }
    }

    #[test]
    fn recovery_scan_decodes_strict_bodies_one_frame_at_a_time() {
        struct BoundedReads(std::io::Cursor<Vec<u8>>);
        impl Read for BoundedReads {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                assert!(out.len() <= 256, "recovery must not buffer the whole WAL");
                self.0.read(out)
            }
        }
        let mut bytes = Vec::new();
        for seq in 1..=1000 {
            bytes.extend_from_slice(
                &encode(
                    seq,
                    1,
                    &WalRecord::StepStarted {
                        step_id: StepId(seq),
                    },
                )
                .unwrap(),
            );
        }
        let size = bytes.len() as u64;
        let scan = scan_prefix(
            BoundedReads(std::io::Cursor::new(bytes.clone())),
            size,
            |_| None::<()>,
        )
        .unwrap();
        assert_eq!(scan.last_seq, Some(1000));
        assert_eq!(scan.valid_end, size);
        let invalid = crate::framing::encode_payload(
            1001,
            1,
            br#"{"type":"step_started","step_id":"wrong"}"#,
        )
        .unwrap();
        bytes.extend_from_slice(&invalid);
        assert!(matches!(
            scan_prefix(
                std::io::Cursor::new(bytes),
                size + invalid.len() as u64,
                |_| None::<()>
            ),
            Err(WalError::Corrupt { .. })
        ));
    }

    #[test]
    fn preencoded_mismatches_leave_wal_and_floor_unchanged() {
        let path = tmp("preencoded-validation");
        let floor = path.with_extension("floor");
        let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
        let record = WalRecord::StepStarted { step_id: StepId(1) };
        let other = WalRecord::StepStarted { step_id: StepId(2) };
        let valid = encode(1, 7, &record).unwrap();
        let mut corrupt = valid.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        let mut extra = valid.clone();
        extra.extend_from_slice(&valid);
        let before = std::fs::read(&path).unwrap();
        for frame in [
            encode(2, 7, &record).unwrap(),
            encode(1, 8, &record).unwrap(),
            encode(1, 7, &other).unwrap(),
            valid[..valid.len() - 1].to_vec(),
            corrupt,
            extra,
        ] {
            assert!(wal.append_preencoded(7, &record, true, frame).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), before);
            assert!(!floor.exists());
            assert_eq!(wal.last_seq(), 0);
        }
        let gated: WalRecord = serde_json::from_value(serde_json::json!({
            "type":"run_cancel_requested", "run_id":"run", "at":"now"
        }))
        .unwrap();
        let ungated =
            crate::framing::encode_payload(1, 7, &serde_json::to_vec(&gated).unwrap()).unwrap();
        assert!(wal.append_preencoded(7, &gated, true, ungated).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(
            !floor.exists(),
            "rejected frames must not publish feature requirements"
        );
        assert_eq!(wal.append_preencoded(7, &record, true, valid).unwrap(), 1);
        assert_eq!(
            FileWal::open(&path).unwrap().replay().unwrap()[0].record,
            record
        );
    }

    #[test]
    fn writer_open_uses_reference_index_and_preserves_standalone_guard() {
        let root = tmp("indexed-open");
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let reference = "0123456789abcdef0123456789abcdef";
        let cut = root.join("agent-data/cuts/cut.json");
        crate::migration::atomic_json(&root, &cut, &serde_json::json!({"intent":null})).unwrap();
        crate::migration::rebuild_fence_index(&root).unwrap();
        // A ready index makes an unrelated cut body unnecessary for this reference.
        std::fs::write(&cut, b"not json").unwrap();
        let path = root.join("wal/session.wal");
        let open = || {
            FileWal::open_with_manifest_and_session_ref(
                &path,
                root.join("manifest"),
                "address",
                "identity",
                reference.into(),
            )
        };
        assert!(open().is_ok());
        assert!(
            FileWal::open(&path).is_err(),
            "unindexed opens must retain the full guard"
        );
        std::fs::write(
            path.with_extension("moving"),
            br#"{"cutId":"cut","endOffset":0,"lineage_id":"id"}"#,
        )
        .unwrap();
        assert!(
            open().is_err(),
            "indexed opens must still check moving sidecars"
        );
    }

    /// The writer refuses to open a session that a pending move intent names by its shared
    /// reference, and creates no file. Store-level checks normally run first; this calls the
    /// writer directly so its own guard is the only one in the way.
    #[test]
    fn writer_open_refuses_a_pending_move_intent_naming_its_reference() {
        let root = tmp("intent-open");
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let fenced = "fedcba9876543210fedcba9876543210";
        crate::migration::atomic_json(
            &root,
            &root.join("agent-data/cuts/cut.json"),
            &serde_json::json!({
                "refs": [{"ref_kind": "broca-session", "ref": fenced}],
                "intent": {"op": "activate", "request": {}},
            }),
        )
        .unwrap();
        let path = root.join("wal/session.wal");
        let open = |reference: &str| {
            FileWal::open_with_manifest_and_session_ref(
                &path,
                root.join("manifest"),
                "address",
                "identity",
                reference.into(),
            )
        };
        assert!(matches!(
            open(fenced),
            Err(WalError::SessionMoving { cut_id }) if cut_id == "cut"
        ));
        assert!(!path.exists(), "a refused open must not create the WAL");
        // The intent names only its own reference; another session at this path opens.
        assert!(open("0123456789abcdef0123456789abcdef").is_ok());
    }

    fn tenure_identity(root: &str, harness: &str, session: &str) -> BindIdentity {
        BindIdentity::new(root, harness, session)
    }

    fn tenure_domain() -> crate::writer_tenure::WalLeaseDomain {
        crate::writer_tenure::WalLeaseDomain::new(PathBuf::from("/leases/test"))
    }

    /// Unwrap a tenure-backed open without requiring `Debug` on the writer.
    fn unwrap_owned(result: Result<OwnedWriterWal, OwnedOpenError>) -> OwnedWriterWal {
        match result {
            Ok(wal) => wal,
            Err(error) => panic!("owned open failed: {error:?}"),
        }
    }

    /// Expect a tenure-backed open refusal without requiring `Debug` on the writer.
    fn expect_owned_err(
        result: Result<OwnedWriterWal, OwnedOpenError>,
        context: &str,
    ) -> OwnedOpenError {
        match result {
            Ok(_) => panic!("owned open unexpectedly succeeded: {context}"),
            Err(error) => error,
        }
    }

    /// Sorted `(relative path, byte length)` snapshot of every file under `dir`.
    fn dir_snapshot(dir: &Path) -> Vec<(PathBuf, u64)> {
        let mut files = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(current) = stack.pop() {
            for entry in std::fs::read_dir(&current).unwrap() {
                let entry = entry.unwrap();
                let file_type = entry.file_type().unwrap();
                if file_type.is_dir() {
                    stack.push(entry.path());
                } else {
                    files.push((
                        entry.path().strip_prefix(dir).unwrap().to_path_buf(),
                        entry.metadata().unwrap().len(),
                    ));
                }
            }
        }
        files.sort();
        files
    }

    #[test]
    fn owned_open_rejects_missing_or_mismatched_tenure_before_mutation() {
        let dir = tmp("owned-tenure-refusal");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.wal");
        let _ = std::fs::remove_file(&path);
        // Existing bytes must survive a refused open untouched.
        let mut seed = FileWal::open(&path).unwrap();
        seed.append(7, &run_started(0), true).unwrap();
        drop(seed);
        let before_bytes = std::fs::read(&path).unwrap();
        let before_entries = dir_snapshot(&dir);
        assert!(!before_bytes.is_empty());

        let session_a = tenure_identity("/project", "runner", "session-a");
        let session_b = tenure_identity("/project", "runner", "session-b");
        let domain = tenure_domain();

        // A tenure minted for another session refuses before any mutation:
        // the check runs before directory creation, torn-tail truncation,
        // lineage initialization and the first append.
        let tenure_b = domain.mint_for_test(&session_b, 41).unwrap();
        let error = expect_owned_err(
            OwnedWriterWal::open(&path, &session_a, "ref-a".to_string(), tenure_b),
            "a tenure for session-b must not open session-a",
        );
        assert!(
            matches!(
                error.cause(),
                OwnedOpenCause::Tenure(TenureError::SessionMismatch { .. })
            ),
            "unexpected refusal: {:?}",
            error.cause()
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before_bytes,
            "a refused owned open must not rewrite bytes"
        );
        assert_eq!(
            dir_snapshot(&dir),
            before_entries,
            "a refused owned open must not add directory entries"
        );

        // On an absent path the refused open creates nothing.
        let absent = dir.join("absent.wal");
        let tenure_b = domain.mint_for_test(&session_b, 42).unwrap();
        assert!(OwnedWriterWal::open(&absent, &session_a, "ref-a".to_string(), tenure_b).is_err());
        assert!(
            !absent.exists(),
            "a refused owned open must not create the WAL"
        );
        assert_eq!(dir_snapshot(&dir), before_entries);

        // A moving session refuses the owned open before creating anything:
        // omitting the tenure is not a bypass, it is a different,
        // still-guarded constructor (see `standalone_append_still_checks_move_fence`).
        let moving_dir = tmp("owned-tenure-moving");
        std::fs::create_dir_all(&moving_dir).unwrap();
        let moving_path = moving_dir.join("session.wal");
        std::fs::write(
            moving_path.with_extension("moving"),
            br#"{"cutId":"cut","endOffset":0,"lineage_id":"id"}"#,
        )
        .unwrap();
        let tenure_a = domain.mint_for_test(&session_a, 43).unwrap();
        let error = expect_owned_err(
            OwnedWriterWal::open(&moving_path, &session_a, "ref-a".to_string(), tenure_a),
            "a moving session must refuse the owned open",
        );
        assert!(
            matches!(
                error.cause(),
                OwnedOpenCause::Wal(WalError::SessionMoving { .. })
            ),
            "unexpected refusal: {:?}",
            error.cause()
        );
        assert!(
            !moving_path.exists(),
            "a move-refused owned open must not create the WAL"
        );

        // The consumed tenures are gone, but a correctly bound tenure opens:
        // nothing about the refusals poisoned the log.
        let tenure_a = domain.mint_for_test(&session_a, 44).unwrap();
        let wal = unwrap_owned(OwnedWriterWal::open(
            &path,
            &session_a,
            "ref-a".to_string(),
            tenure_a,
        ));
        assert_eq!(wal.epoch(), 44);
        assert_eq!(wal.session(), &session_a);
        assert_eq!(wal.last_seq(), 1);
        let outcome = wal.close_into_tenure();
        assert!(outcome.sync_result.is_ok());
        assert_eq!(outcome.tenure.epoch(), 44);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn standalone_append_still_checks_move_fence() {
        // The standalone compatibility path has no tenure to cache a verdict
        // under, so its per-append move guard must stay exactly where it is:
        // planting a freeze after open refuses the very next append.
        let dir = tmp("standalone-guard");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.wal");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open_with_manifest_and_session_ref(
            &path,
            dir.join("manifest"),
            "address",
            "identity",
            "ref-1".to_string(),
        )
        .unwrap();
        wal.append(1, &run_started(0), true).unwrap();
        let clean_len = std::fs::metadata(&path).unwrap().len();
        std::fs::write(
            path.with_extension("moving"),
            br#"{"cutId":"cut","endOffset":0,"lineage_id":"id"}"#,
        )
        .unwrap();
        let error = wal
            .append(1, &run_started(1), true)
            .expect_err("a standalone append must re-check the move fence");
        assert!(
            matches!(error, WalError::SessionMoving { .. }),
            "got {error:?}"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            clean_len,
            "a refused standalone append must not change bytes"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn owned_append_uses_the_open_time_move_verdict() {
        // The owned-path fast path: the open-time guard ran under the held
        // tenure, so later appends skip the repeated migration probes while
        // that verdict still covers this path and reference. Only the
        // consuming owned constructor selects this mode, which nothing mints
        // in production yet, and the standalone path above keeps its
        // per-append guard.
        let dir = tmp("owned-cached-verdict");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.wal");
        let _ = std::fs::remove_file(&path);
        let session = tenure_identity("/project", "runner", "session-a");
        let tenure = tenure_domain().mint_for_test(&session, 7).unwrap();
        let mut wal = unwrap_owned(OwnedWriterWal::open(
            &path,
            &session,
            "ref-1".to_string(),
            tenure,
        ));
        wal.append(&run_started(0), true).unwrap();
        // Freeze the session after open: the cached verdict still authorizes
        // the tenure-backed append, while a standalone handle refuses.
        std::fs::write(
            path.with_extension("moving"),
            br#"{"cutId":"cut","endOffset":0,"lineage_id":"id"}"#,
        )
        .unwrap();
        wal.append(&run_started(1), true)
            .expect("the open-time verdict covers the held tenure's appends");
        assert_eq!(wal.last_seq(), 2);
        let outcome = wal.close_into_tenure();
        assert!(outcome.sync_result.is_ok());
        assert_eq!(replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap().len(), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn owned_close_disables_wal_before_releasing_lease() {
        // Normal shutdown settles under the lease, then disables the writer,
        // then hands back the still-held tenure: the final barrier is the
        // last filesystem effect, and the returned owner is the held one.
        let dir = tmp("owned-close");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.wal");
        let _ = std::fs::remove_file(&path);
        let session = tenure_identity("/project", "runner", "session-a");
        let domain = tenure_domain();
        let tenure = domain.mint_for_test(&session, 41).unwrap();
        let mut wal = unwrap_owned(OwnedWriterWal::open(
            &path,
            &session,
            "ref-1".to_string(),
            tenure,
        ));
        wal.append(&run_started(0), false).unwrap();
        wal.append(&run_started(1), true).unwrap();
        OWNED_CLOSE_SETTLES.with(|count| count.set(0));
        let outcome = wal.close_into_tenure();
        assert!(
            outcome.sync_result.is_ok(),
            "close settles queued appends under the lease"
        );
        assert_eq!(
            OWNED_CLOSE_SETTLES.with(|count| count.get()),
            1,
            "the consuming close performs the final barrier exactly once"
        );
        assert_eq!(outcome.tenure.epoch(), 41);
        assert_eq!(outcome.tenure.session(), &session);
        assert_eq!(outcome.tenure.lease_root(), tenure_domain().lease_root());
        // The close is consuming, so no handle survives that could append
        // past the handoff; both records, including the unsynced one, are
        // durable and readable.
        let replayed = replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].fence, 41);
        assert_eq!(replayed[1].fence, 41);
        // The tenure is released only now: a fresh tenure reopens the log
        // where the closed writer left it.
        let tenure = domain.mint_for_test(&session, 42).unwrap();
        let wal = unwrap_owned(OwnedWriterWal::open(
            &path,
            &session,
            "ref-1".to_string(),
            tenure,
        ));
        assert_eq!(wal.last_seq(), 2);
        let outcome = wal.close_into_tenure();
        assert!(outcome.sync_result.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn owned_open_failure_and_panic_release_only_after_close() {
        let dir = tmp("owned-open-cleanup");
        std::fs::create_dir_all(&dir).unwrap();
        let session = tenure_identity("/project", "runner", "session-a");
        let domain = tenure_domain();

        // An I/O failure during open leaves no writable handle behind: the
        // half-built descriptor drops before tenure teardown, and a fresh
        // tenure opens the real path cleanly.
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let tenure = domain.mint_for_test(&session, 7).unwrap();
        let error = expect_owned_err(
            OwnedWriterWal::open(
                blocker.join("session.wal"),
                &session,
                "ref-1".to_string(),
                tenure,
            ),
            "opening under a file must fail",
        );
        assert!(
            matches!(error.cause(), OwnedOpenCause::Wal(_)),
            "unexpected failure: {:?}",
            error.cause()
        );
        let path = dir.join("session.wal");
        let tenure = domain.mint_for_test(&session, 8).unwrap();
        let mut wal = unwrap_owned(OwnedWriterWal::open(
            &path,
            &session,
            "ref-1".to_string(),
            tenure,
        ));
        wal.append(&run_started(0), true).unwrap();
        wal.close_into_tenure();
        let before_bytes = std::fs::read(&path).unwrap();

        // A panic after recovery unwinds through the half-returned writer:
        // field order drops the descriptor before tenure teardown, so the
        // log keeps its verified prefix and a fresh tenure reopens it.
        OWNED_OPEN_PANIC_AFTER_RECOVERY.with(|flag| flag.set(true));
        let tenure = domain.mint_for_test(&session, 9).unwrap();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            OwnedWriterWal::open(&path, &session, "ref-1".to_string(), tenure)
        }));
        assert!(caught.is_err(), "the injected panic must unwind");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before_bytes,
            "a panic during open must not change bytes"
        );
        let tenure = domain.mint_for_test(&session, 10).unwrap();
        let wal = unwrap_owned(OwnedWriterWal::open(
            &path,
            &session,
            "ref-1".to_string(),
            tenure,
        ));
        assert_eq!(wal.last_seq(), 1);
        let outcome = wal.close_into_tenure();
        assert!(outcome.sync_result.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn first_episode_discriminator_reads_only_its_bounded_prefix() {
        struct Count {
            inner: std::io::Cursor<Vec<u8>>,
            bytes: usize,
        }
        impl Read for Count {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let n = self.inner.read(out)?;
                self.bytes += n;
                Ok(n)
            }
        }
        for record_first in [false, true] {
            let path = tmp("first-episode-prefix");
            let first: WalRecord = serde_json::from_str(include_str!(
                "../tests/golden/pre_step_transforms/run_started.json"
            ))
            .unwrap();
            let mut wal = FileWal::open(&path)
                .unwrap()
                .with_feature_floor(path.with_extension("floor"));
            wal.append(1, &first, true).unwrap();
            let prefix = std::fs::metadata(&path).unwrap().len();
            wal.append(
                1,
                &WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(2),
                    assistant_message: broca_protocol::AssistantMessage {
                        message_id: broca_protocol::MessageId("tail".into()),
                        content: vec![broca_protocol::ContentBlock::Text {
                            text: "x".repeat(8192),
                        }],
                    }
                    .into(),
                    usage: Default::default(),
                    finish_reason: None,
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
                true,
            )
            .unwrap();
            drop(wal);
            if record_first {
                let bytes = std::fs::read(&path).unwrap();
                std::fs::write(&path, &bytes[crate::framing::LINEAGE_FRAME_LEN..]).unwrap();
            }
            assert!(matches!(
                replay_file(&path, prefix),
                Err(WalError::TooLarge { .. })
            ));
            assert_eq!(
                read_first_run_started(&path, prefix)
                    .unwrap()
                    .unwrap()
                    .record,
                first
            );
            let bytes = std::fs::read(&path).unwrap();
            let total = bytes.len();
            let mut counted = Count {
                inner: std::io::Cursor::new(bytes),
                bytes: 0,
            };
            assert_eq!(
                read_first_run_started_from(&mut counted, prefix)
                    .unwrap()
                    .unwrap()
                    .record,
                first
            );
            assert!(
                counted.bytes <= prefix as usize && counted.bytes < total,
                "classification read the tail: {} of {total}",
                counted.bytes
            );
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn first_episode_discriminator_refuses_an_inconclusive_prefix() {
        let path = tmp("first-episode-limit");
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &WalRecord::StepStarted { step_id: StepId(1) }, true)
            .unwrap();
        let prefix = std::fs::metadata(&path).unwrap().len();
        wal.append(1, &run_started(2), true).unwrap();
        drop(wal);
        assert!(matches!(
            read_first_run_started(&path, prefix),
            Err(WalError::TooLarge { .. })
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn file_range_access_distinguishes_absent_and_complete() {
        let path = tmp("range-completeness");
        let _ = std::fs::remove_file(&path);
        let absent = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert!(absent.records.is_empty());
        assert!(matches!(absent.records(), super::WalRangeRecords::Absent));
        assert!(matches!(
            absent.into_records(),
            super::WalRangeRecords::Absent
        ));
        std::fs::write(&path, []).unwrap();
        let complete = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert!(
            matches!(complete.records(), super::WalRangeRecords::Complete(records) if records.is_empty())
        );
        assert!(
            matches!(complete.into_records(), super::WalRangeRecords::Complete(records) if records.is_empty())
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn in_memory_ranges_have_unknown_completeness() {
        for range in [
            super::WalRangeRead::without_anchors(Vec::new()),
            super::WalRangeRead::with_anchors(Vec::new(), Vec::new()),
        ] {
            assert!(matches!(
                range.records(),
                super::WalRangeRecords::Unknown(_)
            ));
            assert!(matches!(
                range.into_records(),
                super::WalRangeRecords::Unknown(_)
            ));
        }
    }

    use super::*;
    use broca_protocol::{StepId, Usage};

    fn tmp(tag: &str) -> PathBuf {
        broca_test_support::scratch_root().join(format!(
            "broca-wal-{}-{}.log",
            std::process::id(),
            tag
        ))
    }

    fn run_started(n: u64) -> WalRecord {
        WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(n),
            assistant_message: (broca_protocol::content::AssistantMessage {
                message_id: broca_protocol::MessageId(format!("m{n}")),
                content: vec![],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    #[test]
    fn unretained_append_never_crosses_recovery_cap() {
        assert_eq!(DEFAULT_MAX_READ_BYTES, 1_073_741_824);
        let path = tmp("ordinary-byte-cap");
        let _ = std::fs::remove_file(&path);
        let cap = 2048;
        let mut wal = FileWal::open(&path).unwrap().with_recovery_cap(cap);
        let first = WalRecord::StepStarted { step_id: StepId(1) };
        wal.append(1, &first, true).unwrap();
        let before = std::fs::read(&path).unwrap();
        let oversized = run_started(2);
        let mut huge = encode(2, 1, &oversized).unwrap();
        huge.resize(cap as usize + 1, 0);
        assert!(matches!(
            wal.append_preencoded(1, &oversized, false, huge),
            Err(WalError::TooLarge { .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(wal.last_seq(), 1);
        let terminal = crate::size_limit::scoped_terminal(
            cap,
            crate::size_limit::SizeScope::Session,
            Some(1),
            Default::default(),
            0,
        );
        wal.append(1, &terminal, true).unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() <= cap);
        drop(wal);
        assert_eq!(FileWal::open(&path).unwrap().replay().unwrap().len(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn full_length_unsynced_tail_recovers_verified_prefix() {
        for (tag, zero, lineage) in [
            ("digest-tail", false, false),
            ("zero-tail", true, false),
            ("lineage-tail", false, true),
        ] {
            let path = tmp(tag);
            let _ = std::fs::remove_file(&path);
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
            let prefix = std::fs::read(&path).unwrap();
            let id = wal.lineage_id.unwrap();
            wal.append(1, &run_started(2), false).unwrap();
            drop(wal);
            let mut bytes = std::fs::read(&path).unwrap();
            let record_end = bytes.len() - crate::framing::LINEAGE_FRAME_LEN;
            if zero {
                bytes[prefix.len()..record_end].fill(0);
            } else {
                bytes[record_end - 1] ^= 0xff;
            }
            if !lineage {
                bytes.truncate(record_end);
            }
            std::fs::write(&path, &bytes).unwrap();
            assert_eq!(replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap().len(), 1);
            assert_eq!(std::fs::read(&path).unwrap(), bytes, "read never repairs");
            let mut reopened = FileWal::open(&path).unwrap();
            assert_eq!(reopened.lineage_id, Some(id));
            assert_eq!(std::fs::read(&path).unwrap(), prefix);
            reopened.append(1, &run_started(2), true).unwrap();
            assert_eq!(reopened.replay().unwrap().len(), 2);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn damaged_length_before_valid_record_is_not_a_torn_tail() {
        check_damaged_length_before_valid_record(false);
    }

    #[cfg(unix)]
    #[test]
    fn anchored_damaged_length_before_valid_record_is_not_a_torn_tail() {
        check_damaged_length_before_valid_record(true);
    }

    fn check_damaged_length_before_valid_record(anchored: bool) {
        let path = tmp(if anchored {
            "anchored-bad-length-before-record"
        } else {
            "bad-length-before-record"
        });
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for seq in 1..=3 {
            wal.append(1, &run_started(seq), true).unwrap();
        }
        drop(wal);
        let mut bytes = std::fs::read(&path).unwrap();
        // Layout: lineage frame, record 1, lineage frame, record 2. Record 1 ends where its own
        // header says; record 2 starts one lineage frame later. Live-file anchors exist only on Unix,
        // so the anchor is a cross-check where present, never the source of the offset.
        let first = crate::framing::LINEAGE_FRAME_LEN;
        let first_len = u32::from_le_bytes(bytes[first..first + 4].try_into().unwrap()) as usize;
        let first_end = first + crate::framing::HEADER_LEN + first_len;
        let anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor;
        if let Some(anchor) = anchor {
            assert_eq!(anchor.byte_end as usize, first_end);
        }
        let second = first_end + crate::framing::LINEAGE_FRAME_LEN;
        bytes[second..second + 4].copy_from_slice(&4096u32.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            replay_file(&path, DEFAULT_MAX_READ_BYTES),
            Err(WalError::Corrupt {
                source: FrameError::NonTailIncompleteFrame,
                ..
            })
        ));
        // Live-file byte anchors require a stable file identity, available only on Unix.
        if anchored {
            let anchor = anchor.expect("anchored corruption test requires a stable file identity");
            assert!(
                read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&anchor)).is_err()
            );
        }
        assert!(read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, None).is_err());
        assert!(FileWal::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn corrupt_record_before_newer_writer_is_not_discarded() {
        let path = tmp("bad-before-newer");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), true).unwrap();
        drop(wal);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0; crate::framing::HEADER_LEN]).unwrap();
        drop(file);
        append_newer_writer_frame(&path, 2);
        assert!(matches!(
            FileWal::open(&path),
            Err(WalError::Corrupt { .. })
        ));
        let _ = std::fs::remove_file(path);
    }

    fn intact_rejected_frames() -> Vec<Vec<u8>> {
        [
            "010000000102000000000000000100000000000000e36c2cb768b1bc82603d9f2b9b5887e8e6ef59c06a9cff1568337df0a85031f87b",
            "000000000200000000000000000000000000000000401da6ea6a614dd6d773b1ba84c91ee6d981827bd0bdc5188aa2e5dbffba558c",
            "020000000502000000000000000100000000000000dede443b9e38d392c3c40582d40136eada16de948804578ca1f6befda78053be7b7d",
        ].iter().map(|hex| hex.as_bytes().as_chunks::<2>().0.iter().map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()).collect()
    }

    #[test]
    fn intact_rejected_tail_is_never_repaired() {
        for (index, frame) in intact_rejected_frames().into_iter().enumerate() {
            assert!(crate::framing::digest_verifies(&frame));
            let path = tmp(&format!("intact-rejected-tail-{index}"));
            let _ = std::fs::remove_file(&path);
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
            drop(wal);
            OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(&frame)
                .unwrap();
            let before = std::fs::read(&path).unwrap();
            assert!(replay_file(&path, DEFAULT_MAX_READ_BYTES).is_err());
            assert!(FileWal::open(&path).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), before);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn corruption_before_intact_rejected_frame_is_fatal() {
        let path = tmp("corrupt-before-intact-rejected");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), true).unwrap();
        drop(wal);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0; crate::framing::HEADER_LEN]).unwrap();
        file.write_all(&intact_rejected_frames()[2]).unwrap();
        drop(file);
        let before = std::fs::read(&path).unwrap();
        assert!(FileWal::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn conflicting_lineage_after_damage_is_never_repaired() {
        for legacy in [false, true] {
            let path = tmp(&format!("foreign-lineage-after-damage-{legacy}"));
            let _ = std::fs::remove_file(&path);
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
            let prefix_id = wal.lineage_id.unwrap();
            drop(wal);
            if legacy {
                std::fs::write(&path, encode(1, 1, &run_started(1)).unwrap()).unwrap();
            }
            let first = WalLineageId::from_bytes([0x5a; 16]);
            let second = WalLineageId::from_bytes([0x5b; 16]);
            let foreign = if prefix_id == first { second } else { first };
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0; crate::framing::HEADER_LEN]).unwrap();
            file.write_all(&encode_lineage(if legacy { first } else { foreign }))
                .unwrap();
            if legacy {
                file.write_all(&encode_lineage(second)).unwrap();
            }
            drop(file);
            let before = std::fs::read(&path).unwrap();
            assert!(replay_file(&path, DEFAULT_MAX_READ_BYTES).is_err());
            assert!(FileWal::open(&path).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), before);
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn intact_rejected_tail_bytes_are_pinned() {
        use sha2::{Digest, Sha256};
        let frames = intact_rejected_frames();
        for (frame, digest) in frames.iter().zip([
            "fc173954f8d37c3ab05dca617977688791396ed3ce890761a8ec309fa4a61003",
            "aadca446961718a022df99918817d44dc7b82e1d6a17bcda15982600f8ffd3a0",
            "de7a77e7ce353f876cf1673c79fa5075406d9552a010fe1002d4b8600e306299",
        ]) {
            assert_eq!(format!("{:x}", Sha256::digest(frame)), digest);
        }
        assert!(matches!(
            crate::framing::decode_next(&frames[0]),
            DecodeStep::Corrupt(FrameError::Deserialize(_))
        ));
        assert!(matches!(
            crate::framing::decode_next(&frames[1]),
            DecodeStep::Corrupt(FrameError::InvalidLineageFrame { .. })
        ));
        assert!(matches!(
            crate::framing::decode_next(&frames[2]),
            DecodeStep::Corrupt(FrameError::UnsupportedVersion { ver: 5 })
        ));
    }

    #[test]
    fn full_length_corrupt_tail_bytes_pin_decoder_classification() {
        // Pin zero-filled headers, a damaged digest and an incomplete declared
        // payload independently of replay's decision to discard only a final tail.
        let mut bad_length = [0u8; crate::framing::HEADER_LEN];
        bad_length[..4].copy_from_slice(&[0, 16, 0, 0]);
        bad_length[4] = 1;
        assert!(matches!(
            crate::framing::decode_next(&bad_length),
            DecodeStep::TornTail
        ));
        let mut bad_digest = [0u8; crate::framing::HEADER_LEN];
        bad_digest[4] = 1;
        bad_digest[5] = 2;
        bad_digest[13] = 1;
        assert!(matches!(
            crate::framing::decode_next(&bad_digest),
            DecodeStep::Corrupt(FrameError::DigestMismatch { seq: 2 })
        ));
        assert!(matches!(
            crate::framing::decode_next(&[0u8; crate::framing::HEADER_LEN]),
            DecodeStep::Corrupt(FrameError::UnsupportedVersion { ver: 0 })
        ));
    }

    fn expect_sequence_gap<T>(result: Result<T, WalError>, expected_seq: u64, seq: u64) {
        match result {
            Err(WalError::SequenceGap {
                expected_seq: actual_expected,
                seq: actual,
            }) => assert_eq!((actual_expected, actual), (expected_seq, seq)),
            Err(error) => panic!("expected sequence-gap error, got {error}"),
            Ok(_) => panic!("expected sequence-gap error, got success"),
        }
    }

    #[cfg(unix)]
    fn large_step(n: u64, text_bytes: usize) -> WalRecord {
        WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(n),
            assistant_message: (broca_protocol::content::AssistantMessage {
                message_id: broca_protocol::MessageId(format!("large-m{n}")),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "x".repeat(text_bytes),
                }],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    #[test]
    fn append_replay_round_trip_and_seq() {
        let path = tmp("rt");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        let s0 = wal.append(1, &run_started(0), true).unwrap();
        let s1 = wal
            .append(1, &WalRecord::TurnFinished { step_id: StepId(1) }, true)
            .unwrap();
        assert_eq!((s0, s1), (1, 2), "seq is 1-based");
        assert_eq!(wal.last_seq(), 2);

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].seq, 1);
        assert_eq!(
            replayed[1].record,
            WalRecord::TurnFinished { step_id: StepId(1) }
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Build a WAL carrying TWO identities: a real single-lineage file, then a foreign
    /// identity frame appended after it -- the shape a splice or partial overwrite leaves.
    fn spliced_lineage_wal(tag: &str) -> PathBuf {
        let path = tmp(tag);
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), true).unwrap();
        drop(wal);
        let foreign = mint_lineage_id().unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut file, &crate::framing::encode_lineage(foreign)).unwrap();
        path
    }

    /// Both `replay_file` and `read_records_range` must reject conflicting lineage identities.
    /// Separate tests distinguish a missing check in one decoder from a missing invariant in both.
    ///
    /// `replay_file` reaches `decode_bytes_seeded`.
    #[test]
    fn replay_file_refuses_a_wal_carrying_two_lineage_identities() {
        let path = spliced_lineage_wal("lineage-conflict-replay");
        match replay_file(&path, DEFAULT_MAX_READ_BYTES) {
            Err(WalError::ConflictingLineageIds { .. }) => {}
            other => panic!("replay_file must refuse a spliced lineage, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    /// `read_records_range` reaches `decode_anchored_suffix` -- the decoder behind the
    /// anchored reads `session.read` uses, so this is the arm whose silence would be the
    /// common case rather than the rare one.
    #[test]
    fn anchored_range_read_refuses_a_wal_carrying_two_lineage_identities() {
        let path = spliced_lineage_wal("lineage-conflict-range");
        match read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None) {
            Err(WalError::ConflictingLineageIds { .. }) => {}
            other => panic!("read_records_range must refuse a spliced lineage, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    /// THE CONTROL, and it is what makes the test above mean "conflict" rather than "two
    /// frames". Every append writes a trailing lineage frame, so a healthy multi-append WAL
    /// already contains several -- a check that refused on COUNT would refuse every WAL in
    /// the tree and this suite would still be green without it.
    #[test]
    fn repeated_identical_lineage_frames_are_accepted() {
        let path = tmp("lineage-repeat");
        let _ = std::fs::remove_file(&path);

        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), true).unwrap();
        let same = wal.lineage_id.expect("a fresh WAL mints an identity");
        drop(wal);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut file, &crate::framing::encode_lineage(same)).unwrap();
        std::io::Write::write_all(&mut file, &crate::framing::encode_lineage(same)).unwrap();
        drop(file);

        assert_eq!(replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap().len(), 1);
        let range = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert_eq!(range.lineage_id, Some(same));
        assert_eq!(range.records.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    /// A tail read decodes only the end of the file: it returns the newest records in
    /// sequence and the lineage from the first frame, without falling back to a read from the
    /// start. A window larger than the file reads it whole.
    #[test]
    fn tail_read_returns_the_newest_records_without_decoding_the_prefix() {
        let path = tmp("tail-read");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for _ in 0..2000 {
            wal.append(1, &run_started(1), false).unwrap();
        }
        wal.sync().unwrap();
        drop(wal);
        let size = std::fs::metadata(&path).unwrap().len();
        let lineage = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None)
            .unwrap()
            .lineage_id;
        let tail = read_records_tail(&path, DEFAULT_MAX_READ_BYTES, 4096).unwrap();
        assert!(
            !tail.from_start,
            "a 4 KiB window must find a frame boundary"
        );
        assert_eq!(tail.lineage_id, lineage);
        let seqs: Vec<u64> = tail.records.iter().map(|record| record.seq).collect();
        assert!(
            !seqs.is_empty() && seqs.len() < 2000,
            "{} records",
            seqs.len()
        );
        assert_eq!(*seqs.last().unwrap(), 2000);
        assert!(seqs.windows(2).all(|pair| pair[1] == pair[0] + 1));
        let whole = read_records_tail(&path, DEFAULT_MAX_READ_BYTES, size).unwrap();
        assert!(whole.from_start);
        assert_eq!(whole.records.len(), 2000);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fresh_wal_persists_one_lineage_identity_across_reopen() {
        let path = tmp("lineage-id-reopen");
        let _ = std::fs::remove_file(&path);
        let first = {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None)
                .unwrap()
                .lineage_id
                .expect("new WAL has lineage metadata")
        };
        let reopened = FileWal::open(&path).unwrap();
        assert_eq!(reopened.lineage_id, Some(first));
        drop(reopened);
        let reread = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert_eq!(reread.lineage_id, Some(first));
        assert_eq!(reread.records.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bounded_tail_snapshot_ignores_append_after_stat_without_loosening_cap() {
        let path = tmp("bounded-tail-append-after-stat");
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), true).unwrap();
        let mut file = File::open(&path).unwrap();
        let metadata = file.metadata().unwrap();
        let before = std::fs::read(&path).unwrap();
        // Model a writer starting its next frame after the reader captured the length.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0])
            .unwrap();
        let result =
            read_records_tail_snapshot(&mut file, &metadata, metadata.len() + 4096, metadata.len());
        assert!(
            result.is_ok(),
            "concurrent append must not become a safety cap: {result:?}"
        );
        let read = result.unwrap().unwrap();
        assert!(matches!(read.records(), WalRangeRecords::Complete(records) if records.len() == 1));
        assert!(matches!(
            read_records_tail_snapshot(&mut file, &metadata, metadata.len() - 1, metadata.len()),
            Err(WalError::TooLarge { max_bytes, file_size })
                if max_bytes == metadata.len() - 1 && file_size == metadata.len()
        ));
        let after = std::fs::read(&path).unwrap();
        assert_eq!(&after[..before.len()], before.as_slice());
        assert_eq!(after.len(), before.len() + 1);
    }

    #[test]
    fn legacy_wal_mints_lineage_identity_only_on_its_next_append() {
        let path = tmp("lineage-id-legacy-next-write");
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, encode(1, 1, &run_started(1)).unwrap()).unwrap();

        let legacy = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert_eq!(legacy.lineage_id, None);
        assert_eq!(legacy.records.len(), 1);

        let mut wal = FileWal::open(&path).unwrap();
        assert_eq!(
            wal.lineage_id(),
            None,
            "a legacy file is unidentified until it is written"
        );
        wal.append(1, &run_started(2), true).unwrap();
        let live = wal.lineage_id();
        drop(wal);
        let upgraded = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert!(upgraded.lineage_id.is_some());
        assert_eq!(
            live, upgraded.lineage_id,
            "the writer reports the identity it just wrote to disk"
        );
        assert_eq!(
            upgraded
                .records
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn writer_reports_the_lineage_of_the_file_it_appends_to() {
        let path = tmp("lineage-id-writer-accessor");
        let _ = std::fs::remove_file(&path);
        let created = FileWal::open(&path).unwrap();
        let first = created.lineage_id().expect("creation mints an identity");
        drop(created);
        let reopened = FileWal::open(&path).unwrap();
        assert_eq!(reopened.lineage_id(), Some(first));
        drop(reopened);

        // Deleting and recreating the file at the same path is a new lineage.
        std::fs::remove_file(&path).unwrap();
        let recreated = FileWal::open(&path).unwrap();
        let second = recreated
            .lineage_id()
            .expect("recreation mints an identity");
        assert_ne!(first, second);
        drop(recreated);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fence_is_a_forensic_stamp_not_a_gate() {
        let path = tmp("forensic-fence");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();

        wal.append(42, &run_started(0), true).unwrap();
        let stale_seq = wal
            .append(1, &run_started(1), true)
            .expect("an arbitrary stale lease stamp is not an append gate");

        assert_eq!(stale_seq, 2);
        let replayed = wal.replay().unwrap();
        assert_eq!(
            replayed
                .iter()
                .map(|record| record.fence)
                .collect::<Vec<_>>(),
            vec![42, 1],
            "replay preserves lease-tenure stamps for forensic correlation"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen_recovers_next_seq() {
        let path = tmp("reopen");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(0), true).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
        }
        let mut wal = FileWal::open(&path).unwrap();
        assert_eq!(wal.last_seq(), 2);
        let s = wal.append(1, &run_started(2), true).unwrap();
        assert_eq!(s, 3, "next_seq continues after reopen");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn leading_non_one_sequence_fails_full_replay_range_read_and_writer_open() {
        let path = tmp("leading-sequence-gap");
        let _ = std::fs::remove_file(&path);
        let bytes = encode(3, 1, &run_started(3)).unwrap();
        std::fs::write(&path, &bytes).unwrap();

        expect_sequence_gap(replay_file(&path, DEFAULT_MAX_READ_BYTES), 1, 3);
        expect_sequence_gap(
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None),
            1,
            3,
        );
        expect_sequence_gap(FileWal::open(&path), 1, 3);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "a writer-recovery open must not mutate or append after a sequence gap"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn internal_sequence_gap_fails_full_replay_range_read_and_writer_open() {
        let path = tmp("internal-sequence-gap");
        let _ = std::fs::remove_file(&path);
        let mut bytes = encode(1, 1, &run_started(1)).unwrap();
        bytes.extend_from_slice(&encode(3, 1, &run_started(3)).unwrap());
        std::fs::write(&path, &bytes).unwrap();

        expect_sequence_gap(replay_file(&path, DEFAULT_MAX_READ_BYTES), 2, 3);
        expect_sequence_gap(
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None),
            2,
            3,
        );
        expect_sequence_gap(FileWal::open(&path), 2, 3);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "a writer-recovery open must leave every surviving frame available for review"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    fn dir_sync_root(tag: &str) -> PathBuf {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-wal-dir-sync-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        root
    }

    #[cfg(unix)]
    #[test]
    fn unix_directory_sync_failure_never_acknowledges_creation() {
        let root = dir_sync_root("creation-eio");
        let path = root.join("nested/wal/writer.log");
        let probe = SyncProbe::default();
        let mut wal = FileWal::open_with_probe(&path, probe.clone()).unwrap();
        wal.append(1, &run_started(1), false).unwrap();
        let pending = wal.newly_created_dirs.borrow().clone();
        crate::directory_sync::with_failure(&root, || {
            assert!(
                matches!(wal.sync(), Err(WalError::Io(error)) if error.raw_os_error() == Some(5))
            );
            assert!(wal.dir_needs_sync.get());
            assert_eq!(*wal.newly_created_dirs.borrow(), pending);
        });
        wal.sync().unwrap();
        assert!(!wal.dir_needs_sync.get());
        assert!(wal.newly_created_dirs.borrow().is_empty());
        let attempts = probe.dir_syncs.load(std::sync::atomic::Ordering::SeqCst);
        wal.sync().unwrap();
        assert_eq!(
            probe.dir_syncs.load(std::sync::atomic::Ordering::SeqCst),
            attempts
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn reopened_unsynced_creation_syncs_parent_before_recovery_ack() {
        let root = dir_sync_root("reopen-eio");
        let path = root.join("writer.log");
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), false).unwrap();
        drop(wal);
        let recovered = FileWal::open_with_probe(&path, SyncProbe::default()).unwrap();
        crate::directory_sync::with_failure(&root, || {
            assert!(
                recovered.sync().is_err(),
                "a readable prefix does not prove its name durable"
            );
        });
        assert!(recovered.dir_needs_sync.get());
        recovered.sync().unwrap();
        assert!(!recovered.dir_needs_sync.get());
        assert_eq!(recovered.last_seq(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn floor_directory_eio_refuses_gated_append() {
        let root = dir_sync_root("floor-eio");
        let path = root.join("writer.log");
        let floor = root.join(crate::floor::FEATURE_FLOOR_FILENAME);
        let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
        let before = std::fs::read(&path).unwrap();
        crate::directory_sync::with_failure(&root, || {
            for _ in 0..2 {
                assert!(matches!(
                    wal.append(1, &scoped_run_started("eio"), false),
                    Err(WalError::FeatureFloor { .. })
                ));
                assert_eq!(std::fs::read(&path).unwrap(), before);
                assert_eq!(wal.last_seq(), 0);
            }
        });
        assert_eq!(wal.append(1, &scoped_run_started("eio"), true).unwrap(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn first_barrier_syncs_a_new_wal_directory_and_its_existing_parent_once() {
        let state_root = dir_sync_root("new-wal-root");
        let wal_dir = state_root.join("wal");
        let path = wal_dir.join("writer.log");
        let probe = SyncProbe::default();
        let mut wal = FileWal::open_with_probe(&path, probe.clone()).unwrap();

        wal.append(1, &run_started(0), false).unwrap();
        wal.sync().unwrap();
        assert_eq!(
            *probe.dir_sync_paths.lock().unwrap(),
            vec![wal_dir, state_root.clone()],
            "the file directory and the parent containing its new name are synced bottom-up"
        );

        wal.append(1, &run_started(1), false).unwrap();
        wal.sync().unwrap();
        assert_eq!(
            probe.file_syncs.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "both barriers still fsync the file"
        );
        assert_eq!(
            probe.dir_syncs.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the directory-sync pass runs only at the first barrier"
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }

    /// A WAL holds full prompts, tool arguments and tool results. It must not be created
    /// world-readable, and neither must the directories leading to it.
    ///
    /// The failure this pins is silent by construction: `create_dir_all` and a default
    /// `OpenOptions` take the process umask, so on a 0022 install the durable log lands 0644
    /// under 0755 directories and nothing about that is observable from the log's contents.
    #[cfg(unix)]
    #[test]
    fn a_new_wal_and_its_directories_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        // Compare against a CONTROL file created the ordinary way in the same directory.
        // Asserting 0600 alone measures the machine rather than the code: under a 0077 umask
        // the WAL lands 0600 whether or not the explicit mode is applied, and this test stays
        // green with both halves of the fix deleted (verified). The property under test is
        // "this code sets the mode", which is only observable when the ambient default differs
        // -- so the control establishes what the ambient default IS, and the test refuses to
        // report a pass when it cannot tell the two apart.
        let root = dir_sync_root("private-mode");
        let path = root.join("nested").join("wal").join("writer.log");
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(0), false).unwrap();
        wal.sync().unwrap();

        let control_path = path.parent().unwrap().join("ambient-control");
        std::fs::File::create(&control_path).unwrap();
        let control_mode = std::fs::metadata(&control_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_ne!(
            control_mode & 0o077,
            0,
            "the ambient umask already yields {control_mode:o}, so this test cannot tell an \
             explicit mode from the default -- it would pass with the fix removed. Run it under \
             a permissive umask (0022) or the assertion below is decoration."
        );

        let file_mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            file_mode, 0o600,
            "a WAL file must be owner-only, got {file_mode:o}"
        );

        for dir in [root.join("nested").join("wal"), root.join("nested")] {
            let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{dir:?} must be owner-only, got {mode:o}");
        }

        let _ = std::fs::remove_dir_all(&root);
    }

    #[cfg(unix)]
    #[test]
    fn existing_wal_directory_keeps_the_single_parent_sync() {
        let state_root = dir_sync_root("existing-wal-root");
        let wal_dir = state_root.join("wal");
        std::fs::create_dir(&wal_dir).unwrap();
        let path = wal_dir.join("writer.log");
        let probe = SyncProbe::default();
        let mut wal = FileWal::open_with_probe(&path, probe.clone()).unwrap();

        wal.append(1, &run_started(0), false).unwrap();
        wal.sync().unwrap();
        wal.append(1, &run_started(1), false).unwrap();
        wal.sync().unwrap();

        assert_eq!(*probe.dir_sync_paths.lock().unwrap(), vec![wal_dir]);
        assert_eq!(
            probe.dir_syncs.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "later barriers do not repeat the existing parent-directory sync"
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[cfg(unix)]
    #[test]
    fn first_barrier_syncs_every_parent_of_a_deeply_created_chain_bottom_up() {
        let state_root = dir_sync_root("deep-wal-root");
        let a = state_root.join("a");
        let b = a.join("b");
        let wal_dir = b.join("wal");
        let path = wal_dir.join("writer.log");
        let probe = SyncProbe::default();
        let mut wal = FileWal::open_with_probe(&path, probe.clone()).unwrap();

        wal.append(1, &run_started(0), false).unwrap();
        wal.sync().unwrap();

        assert_eq!(
            *probe.dir_sync_paths.lock().unwrap(),
            vec![wal_dir, b, a, state_root.clone()],
            "each directory that gained a new child is synced from the leaf upward"
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn torn_trailing_record_is_discarded_not_fatal() {
        let path = tmp("torn");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(0), true).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
        }
        // Simulate a crash mid-append: append a partial (truncated) record's bytes.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xAA; 10]).unwrap(); // < HEADER_LEN → torn tail
            f.flush().unwrap();
        }
        let wal = FileWal::open(&path).unwrap();
        let replayed = wal.replay().unwrap();
        assert_eq!(
            replayed.len(),
            2,
            "torn tail discarded, committed records intact"
        );
        assert_eq!(
            wal.last_seq(),
            2,
            "next append follows the last VALID record"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn torn_tail_is_truncated_so_resume_appends_survive() {
        // The recovery path: crash mid-append leaves a torn tail, then a RESUME opens the
        // WAL and appends more. Before truncation-on-open, the resume's appends landed AFTER
        // the torn bytes, and a later replay hit a torn frame mid-stream → fail-loud, losing
        // the resume's records. This asserts the torn tail is truncated so the resume's
        // appends are durable + replayable.
        let path = tmp("torn-resume");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(0), true).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
        }
        let size_before_torn = std::fs::metadata(&path).unwrap().len();
        // Simulate a crash mid-append: a partial (sub-header) trailing record.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xAA; 10]).unwrap();
            f.flush().unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > size_before_torn);
        // RESUME: open (must truncate the torn tail) and append a third record.
        {
            let mut wal = FileWal::open(&path).unwrap();
            assert_eq!(wal.last_seq(), 2, "resume follows the last valid record");
            // The torn bytes must be gone so the new append lands right after record 2.
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                size_before_torn,
                "the torn tail was truncated on open"
            );
            let s = wal.append(1, &run_started(2), true).unwrap();
            assert_eq!(s, 3);
        }
        // RE-OPEN: the resume's append must be fully replayable (the original bug lost it).
        let wal = FileWal::open(&path).unwrap();
        let replayed = wal.replay().unwrap();
        assert_eq!(
            replayed.len(),
            3,
            "all three records (incl. the post-torn resume append) replay cleanly"
        );
        assert_eq!(
            replayed.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(wal.last_seq(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tail_after_seq_returns_only_newer() {
        let path = tmp("tail");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..5 {
            wal.append(
                1,
                &WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(i),
                    assistant_message: (broca_protocol::content::AssistantMessage {
                        message_id: broca_protocol::MessageId(format!("m{i}")),
                        content: vec![],
                    })
                    .into(),
                    usage: Usage::default(),
                    finish_reason: None,
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
                false,
            )
            .unwrap();
        }
        // 5 records get seq 1..=5; tail_after_seq(2) = seq 3,4,5.
        let tail = wal.tail_after_seq(2).unwrap();
        assert_eq!(tail.len(), 3, "seq 3, 4, 5");
        assert_eq!(tail[0].seq, 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn range_from_zero_matches_full_replay() {
        let path = tmp("range-differential");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..6 {
            wal.append(1, &run_started(i), false).unwrap();
        }

        let replayed = replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap();
        let ranged = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert_eq!(ranged.records, replayed);
        assert_eq!(ranged.records.len(), 6);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn through_seq_excludes_records_visible_at_physical_eof() {
        let path = tmp("range-through");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..5 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        assert_eq!(replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap().len(), 5);

        let ranged = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, Some(3), None).unwrap();
        assert_eq!(
            ranged
                .records
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "records flushed past the durable safe head must be excluded"
        );
        // Anchors are derived from the unix device+inode identity; other platforms
        // deliberately return none (the fast path is disabled there).
        #[cfg(unix)]
        assert_eq!(ranged.next_anchor.unwrap().seq, 3);
        #[cfg(not(unix))]
        assert_eq!(ranged.next_anchor, None);
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn valid_anchor_fast_path_matches_from_zero_range_scan() {
        let path = tmp("range-anchor-valid");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..2 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        let prefix = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(2), None).unwrap();
        let anchor = prefix.next_anchor.expect("Unix reads produce byte anchors");
        assert_eq!(anchor.seq, 2);

        for i in 2..6 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        let suffix_bytes = std::fs::metadata(&path).unwrap().len() - anchor.byte_end;
        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let anchored =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 2, None, Some(&anchor)).unwrap();
        let anchored_bytes = DECODED_BYTE_COUNT.with(std::cell::Cell::get);
        assert_eq!(anchored_bytes, suffix_bytes);

        let scanned = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 2, None, None).unwrap();
        assert_eq!(anchored, scanned);
        assert_eq!(
            anchored
                .records
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![3, 4, 5, 6]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn single_large_anchored_frame_is_decoded_once_by_both_range_paths() {
        let path = tmp("range-anchor-single-large");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(0), false).unwrap();
        let anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor
            .expect("Unix reads produce byte anchors");
        let large = large_step(1, 1024 * 1024);
        wal.append(1, &large, false).unwrap();
        let suffix_bytes = std::fs::metadata(&path).unwrap().len() - anchor.byte_end;

        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let ranged = read_records_range(
            &path,
            DEFAULT_MAX_READ_BYTES,
            anchor.seq,
            None,
            Some(&anchor),
        )
        .unwrap();
        assert_eq!(DECODED_BYTE_COUNT.with(std::cell::Cell::get), suffix_bytes);
        assert_eq!(ranged.records.len(), 1);
        assert_eq!(ranged.records[0].record, large);

        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let strict = read_records_range_from_valid_anchor(
            &path,
            DEFAULT_MAX_READ_BYTES,
            anchor.seq,
            None,
            &anchor,
        )
        .unwrap()
        .expect("valid anchor");
        assert_eq!(DECODED_BYTE_COUNT.with(std::cell::Cell::get), suffix_bytes);
        assert_eq!(strict, ranged);
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn strict_anchored_range_rejects_a_stale_anchor_without_scanning() {
        let path = tmp("range-anchor-strict-stale");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..3 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        let prefix = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(2), None).unwrap();
        let mut anchor = prefix.anchor_at(2).expect("anchor at requested sequence");
        anchor.file_id ^= 1;

        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let strict =
            read_records_range_from_valid_anchor(&path, DEFAULT_MAX_READ_BYTES, 2, None, &anchor)
                .unwrap();
        assert_eq!(strict, None);
        assert_eq!(
            DECODED_BYTE_COUNT.with(std::cell::Cell::get),
            0,
            "invalid anchors must return control to the full-replay caller without decoding"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn strict_anchored_range_rejects_an_internal_sequence_gap() {
        let path = tmp("range-anchor-strict-gap");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), false).unwrap();
        wal.append(1, &run_started(2), true).unwrap();
        let prefix = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(2), None).unwrap();
        let anchor = prefix.anchor_at(2).expect("anchor at requested sequence");
        drop(wal);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        for seq in [3, 5, 6] {
            file.write_all(&encode(seq, 1, &run_started(seq)).unwrap())
                .unwrap();
        }
        file.sync_all().unwrap();

        let strict = read_records_range_from_valid_anchor(
            &path,
            DEFAULT_MAX_READ_BYTES,
            2,
            Some(6),
            &anchor,
        )
        .unwrap();
        assert_eq!(
            strict, None,
            "a foreign or corrupt file with an internal gap must select full replay"
        );
        expect_sequence_gap(
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 2, Some(6), Some(&anchor)),
            4,
            5,
        );
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn anchored_large_range_skips_prefix_bytes_and_matches_scan() {
        let path = tmp("range-anchor-large");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..1_000 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        let prefix = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(900), None).unwrap();
        let anchor = prefix.anchor_at(900).expect("anchor at requested sequence");

        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let scanned = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 900, None, None).unwrap();
        let scanned_bytes = DECODED_BYTE_COUNT.with(std::cell::Cell::get);
        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let anchored =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 900, None, Some(&anchor)).unwrap();
        let anchored_bytes = DECODED_BYTE_COUNT.with(std::cell::Cell::get);

        assert_eq!(anchored, scanned);
        assert_eq!(anchored.records.len(), 100);
        assert!(
            scanned_bytes > anchored_bytes * 5,
            "anchored decode examined {anchored_bytes} bytes versus {scanned_bytes} from zero"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn anchor_recovers_when_a_torn_next_frame_later_finishes() {
        let path = tmp("range-anchor-torn-grow");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(0), false).unwrap();
        }
        let anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor
            .unwrap();
        let next_frame = encode(2, 1, &run_started(1)).unwrap();
        let split = crate::framing::HEADER_LEN / 2;
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&next_frame[..split]).unwrap();
            file.flush().unwrap();
        }

        let while_torn =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&anchor)).unwrap();
        let scanned_while_torn =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, None).unwrap();
        assert_eq!(while_torn, scanned_while_torn);
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&next_frame[split..]).unwrap();
            file.flush().unwrap();
        }

        let after_growth =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&anchor)).unwrap();
        assert_eq!(
            after_growth
                .records
                .iter()
                .map(|record| record.seq)
                .collect::<Vec<_>>(),
            vec![2]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn stale_anchor_after_file_replacement_falls_back_to_zero() {
        let path = tmp("range-anchor-replaced");
        let old_path = tmp("range-anchor-replaced-old");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&old_path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            for i in 0..3 {
                wal.append(1, &run_started(i), false).unwrap();
            }
        }
        let anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor
            .unwrap();

        std::fs::rename(&path, &old_path).unwrap();
        {
            let mut replacement = FileWal::open(&path).unwrap();
            for i in 10..15 {
                replacement.append(2, &run_started(i), false).unwrap();
            }
        }
        let replacement_id = metadata_file_id(&std::fs::metadata(&path).unwrap()).unwrap();
        assert_ne!(anchor.file_id, replacement_id);

        let anchored =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&anchor)).unwrap();
        let scanned = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, None).unwrap();
        assert_eq!(anchored, scanned);
        assert_eq!(anchored.records.len(), 4);

        let mut wrong_seq = anchored.anchor_at(2).expect("replacement anchor at seq 2");
        wrong_seq.seq += 1;
        let from_wrong_seq =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 2, None, Some(&wrong_seq)).unwrap();
        let scanned_after_two =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 2, None, None).unwrap();
        assert_eq!(from_wrong_seq, scanned_after_two);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&old_path);
    }

    #[cfg(unix)]
    #[test]
    fn anchor_at_mid_frame_or_garbage_falls_back_to_zero() {
        let path = tmp("range-anchor-bad-offset");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..3 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        let anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor
            .unwrap();
        let scanned = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, None).unwrap();

        let mut mid_frame = anchor;
        mid_frame.byte_end += 1;
        let from_mid =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&mid_frame)).unwrap();
        assert_eq!(from_mid, scanned);

        let garbage_start = std::fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
            file.flush().unwrap();
        }
        let garbage = WalByteAnchor {
            byte_end: garbage_start,
            ..anchor
        };
        let from_garbage =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&garbage)).unwrap();
        assert_eq!(from_garbage.records, scanned.records);
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn anchor_beyond_a_shrunken_file_falls_back_to_zero() {
        let path = tmp("range-anchor-shrink");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..4 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        let first = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor
            .unwrap();
        let fourth = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(4), None)
            .unwrap()
            .next_anchor
            .unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(first.byte_end)
            .unwrap();

        let anchored =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 4, None, Some(&fourth)).unwrap();
        let scanned = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 4, None, None).unwrap();
        assert_eq!(anchored, scanned);
        assert!(anchored.records.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[test]
    fn anchored_range_still_fails_loud_on_later_corruption() {
        let path = tmp("range-anchor-corrupt");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..4 {
            wal.append(1, &run_started(i), false).unwrap();
        }
        let first = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor
            .unwrap();
        let third = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(3), None)
            .unwrap()
            .next_anchor
            .unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[third.byte_end as usize - 1] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        let error = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&first))
            .expect_err("a complete corrupt frame after the anchor must fail loud");
        assert!(matches!(error, WalError::Corrupt { .. }));
        let _ = std::fs::remove_file(&path);
    }

    /// Append a gated frame at `seq` that needs a feature no build knows yet, through the real
    /// encoder, as a newer build would.
    fn append_newer_writer_frame(path: &Path, seq: u64) -> u64 {
        let offset = std::fs::metadata(path).unwrap().len();
        let bytes = crate::framing::encode_with_requires(
            seq,
            1,
            &["from-the-future/v1"],
            &run_started(seq),
        )
        .unwrap();
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(&bytes).unwrap();
        offset
    }

    fn expect_newer_writer<T: std::fmt::Debug>(result: Result<T, WalError>, offset: u64) {
        match result {
            Err(WalError::NewerWriter {
                offset: actual,
                ver,
                missing,
            }) => {
                assert_eq!((actual as u64, ver), (offset, 3));
                assert_eq!(missing, vec!["from-the-future/v1".to_string()]);
            }
            other => panic!("expected NewerWriter, got {other:?}"),
        }
    }

    /// Torn-tail repair and a later append leave a gated frame exactly as it was written.
    ///
    /// Repair truncates only the incomplete tail and append only adds bytes after the end;
    /// neither may re-encode a record it read, because `FramedRecord` does not carry the
    /// frame's `requires` and re-encoding would write a version-1 frame with the gate removed.
    /// Asserted on the bytes rather than the decoded record: a frame stripped of its gate
    /// decodes to the very same record.
    #[test]
    fn torn_tail_repair_and_append_keep_a_gated_frame_byte_for_byte() {
        use crate::framing::{
            decode_next, encode, encode_with_requires, DecodeStep, GATED_FRAMING_VERSION,
            TEST_KNOWN_FEATURE,
        };
        let path = tmp("gated-known-repair");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
        }
        let gated = encode_with_requires(2, 1, &[TEST_KNOWN_FEATURE], &run_started(2)).unwrap();
        let gated_at = std::fs::metadata(&path).unwrap().len() as usize;
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&gated).unwrap();
        let intact = std::fs::read(&path).unwrap();
        let torn = encode(3, 1, &run_started(3)).unwrap();
        file.write_all(&torn[..torn.len() - 1]).unwrap();
        drop(file);

        {
            let mut wal = FileWal::open(&path).unwrap();
            assert_eq!(
                std::fs::read(&path).unwrap(),
                intact,
                "repair cut only the torn tail"
            );
            wal.append(1, &run_started(3), true).unwrap();
        }
        let after = std::fs::read(&path).unwrap();
        assert!(after.starts_with(&intact), "append rewrote earlier bytes");
        assert_eq!(&after[gated_at..gated_at + gated.len()], &gated[..]);
        assert_eq!(after[gated_at + 4], GATED_FRAMING_VERSION);
        match decode_next(&after[gated_at..]) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, run_started(2)),
            _ => panic!("the gated frame must still decode in a build that knows its feature"),
        }
        let replayed = replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap();
        assert_eq!(
            replayed.iter().map(|f| f.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let _ = std::fs::remove_file(&path);
    }

    /// A `RunStarted` carrying a frozen scope, decoded from its bytes so this helper does not
    /// change when a field is added to the record.
    fn scoped_run_started(run: &str) -> WalRecord {
        serde_json::from_str(&format!(
            r#"{{"type":"run_started","run_id":"{run}","session":{{"project_root":"/tmp/p","harness":"runner","session":"s"}},"principal":{{"kind":"reserved","module_id":"prefrontal-core"}},"config":{{"model":{{"provider_module_id":"p","model_id":"m"}},"tools":[],"tool_choice":{{"type":"auto"}},"generation":{{}},"response_format":{{"type":"text"}},"context_epoch":0,"renderer_version":1}},"input":[],"origin":{{"kind":"fresh"}},"scope":{{"owner":{{"kind":"reserved","module_id":"prefrontal-core"}},"ref":"head-1","scope_epoch":3}}}}"#
        ))
        .unwrap()
    }

    /// Every file reader decodes a WAL whose `RunStarted` froze a scope: full replay, the
    /// range read, the anchored read behind `session.read` (without falling back to a read
    /// from byte zero), and the writer's own open. The frame on disk is a gated frame requiring
    /// `scope/v1`, and the scope comes back intact on each path.
    #[cfg(unix)]
    #[test]
    fn every_file_reader_decodes_a_scoped_run_started() {
        use crate::framing::{GATED_FRAMING_VERSION, SCOPE_FEATURE};
        let path = tmp("scoped-run-started");
        let _ = std::fs::remove_file(&path);
        let first = scoped_run_started("run-1");
        let second = scoped_run_started("run-2");
        let floor = tmp("scoped-run-started-floor");
        let _ = std::fs::remove_file(&floor);
        let anchor;
        {
            let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
            wal.append(1, &first, true).unwrap();
            anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
                .unwrap()
                .next_anchor
                .unwrap();
            wal.append(1, &second, true).unwrap();
        }
        let bytes = std::fs::read(&path).unwrap();
        let second_at = anchor.byte_end as usize + crate::framing::LINEAGE_FRAME_LEN;
        assert_eq!(bytes[second_at + 4], GATED_FRAMING_VERSION);
        let payload_at = second_at + crate::framing::HEADER_LEN;
        let requires = format!(r#"{{"requires":["{SCOPE_FEATURE}"],"record":"#);
        assert!(bytes[payload_at..].starts_with(requires.as_bytes()));

        let decoded =
            |records: Vec<FramedRecord>| records.into_iter().map(|f| f.record).collect::<Vec<_>>();
        assert_eq!(
            decoded(replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap()),
            vec![first.clone(), second.clone()]
        );
        let WalRangeRecords::Complete(ranged) =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None)
                .unwrap()
                .into_records()
        else {
            panic!("a present file reads as complete");
        };
        assert_eq!(decoded(ranged), vec![first.clone(), second.clone()]);

        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let WalRangeRecords::Complete(anchored) =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&anchor))
                .unwrap()
                .into_records()
        else {
            panic!("a present file reads as complete");
        };
        assert_eq!(decoded(anchored), vec![second.clone()]);
        assert_eq!(
            DECODED_BYTE_COUNT.with(std::cell::Cell::get),
            (bytes.len() as u64) - anchor.byte_end,
            "the anchored read fell back to decoding the WAL from byte zero"
        );
        let WalRecord::RunStarted { scope, .. } = &second else {
            unreachable!()
        };
        assert_eq!(scope.as_ref().unwrap().scope_epoch, 3);

        let mut reopened = FileWal::open(&path).unwrap();
        assert_eq!(reopened.append(1, &run_started(3), true).unwrap(), 3);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&floor);
    }

    /// A `RunStarted` carrying a frozen plan manifest, decoded from its bytes. Unix-only, like
    /// the one test that uses it.
    #[cfg(unix)]
    fn manifest_run_started(run: &str) -> WalRecord {
        serde_json::from_str(
            &include_str!("../tests/fixtures/plan_manifest_run_started.json")
                .trim_end()
                .replace("{RUN_ID}", run),
        )
        .unwrap()
    }

    /// Every file reader decodes a WAL whose `RunStarted` froze a plan manifest: full replay,
    /// the range read, the anchored read behind `session.read` (without falling back to a read
    /// from byte zero), and the writer's own open, which must not treat the frame as a torn tail.
    /// The frame on disk requires `plan-manifest/v1`, the floor names it, and the manifest comes
    /// back intact on each path.
    #[cfg(unix)]
    #[test]
    fn every_file_reader_decodes_a_manifest_run_started() {
        use crate::framing::{GATED_FRAMING_VERSION, PLAN_MANIFEST_FEATURE};
        let path = tmp("manifest-run-started");
        let _ = std::fs::remove_file(&path);
        let first = manifest_run_started("run-1");
        let second = manifest_run_started("run-2");
        let floor = tmp("manifest-run-started-floor");
        let _ = std::fs::remove_file(&floor);
        let anchor;
        {
            let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
            wal.append(1, &first, true).unwrap();
            anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
                .unwrap()
                .next_anchor
                .unwrap();
            wal.append(1, &second, true).unwrap();
        }
        assert_eq!(
            std::fs::read_to_string(&floor).unwrap(),
            format!("{PLAN_MANIFEST_FEATURE}\n")
        );
        let bytes = std::fs::read(&path).unwrap();
        let second_at = anchor.byte_end as usize + crate::framing::LINEAGE_FRAME_LEN;
        assert_eq!(bytes[second_at + 4], GATED_FRAMING_VERSION);
        let payload_at = second_at + crate::framing::HEADER_LEN;
        let requires = format!(r#"{{"requires":["{PLAN_MANIFEST_FEATURE}"],"record":"#);
        assert!(bytes[payload_at..].starts_with(requires.as_bytes()));

        let decoded =
            |records: Vec<FramedRecord>| records.into_iter().map(|f| f.record).collect::<Vec<_>>();
        assert_eq!(
            decoded(replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap()),
            vec![first.clone(), second.clone()]
        );
        let WalRangeRecords::Complete(ranged) =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None)
                .unwrap()
                .into_records()
        else {
            panic!("a present file reads as complete");
        };
        assert_eq!(decoded(ranged), vec![first.clone(), second.clone()]);

        DECODED_BYTE_COUNT.with(|count| count.set(0));
        let WalRangeRecords::Complete(anchored) =
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&anchor))
                .unwrap()
                .into_records()
        else {
            panic!("a present file reads as complete");
        };
        assert_eq!(decoded(anchored), vec![second.clone()]);
        assert_eq!(
            DECODED_BYTE_COUNT.with(std::cell::Cell::get),
            (bytes.len() as u64) - anchor.byte_end,
            "the anchored read fell back to decoding the WAL from byte zero"
        );
        let WalRecord::RunStarted { manifest, .. } = &second else {
            unreachable!()
        };
        assert_eq!(manifest.as_ref().unwrap().fetches.len(), 3);

        let mut reopened = FileWal::open(&path).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "open must keep the gated frame, not cut it as a torn tail"
        );
        assert_eq!(reopened.append(1, &run_started(3), true).unwrap(), 3);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&floor);
    }

    /// A record that requires a WAL feature is appended only after the feature floor lists
    /// that feature. When the floor cannot be raised (none configured, or the write fails) the
    /// append is refused and the WAL is left byte for byte as it was, so no gated frame can
    /// ever be on disk without the floor naming it. When it can, the floor lists `scope/v1` by
    /// the time the append returns. A record that requires nothing never touches the floor.
    #[test]
    fn a_gated_append_raises_the_floor_first_and_writes_nothing_when_it_cannot() {
        let path = tmp("floor-order");
        let _ = std::fs::remove_file(&path);
        let floor = tmp("floor-order-floor");
        let _ = std::fs::remove_dir_all(&floor);
        let _ = std::fs::remove_file(&floor);
        let scoped = scoped_run_started("run-1");

        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(1), true).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(
            wal.append(1, &scoped, true),
            Err(WalError::FeatureFloor { path: None, .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before, "no floor, no frame");
        drop(wal);

        // A directory where the floor file should be: the raise fails.
        std::fs::create_dir_all(&floor).unwrap();
        let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
        match wal.append(1, &scoped, true) {
            Err(WalError::FeatureFloor {
                path: Some(refused),
                requires,
                ..
            }) => {
                assert_eq!(refused, floor);
                assert_eq!(requires, vec![crate::framing::SCOPE_FEATURE.to_owned()]);
            }
            other => panic!("expected a floor refusal, got {other:?}"),
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "a gated frame was written although the floor could not be raised"
        );
        drop(wal);
        std::fs::remove_dir(&floor).unwrap();

        let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
        wal.append(1, &run_started(2), true).unwrap();
        assert!(
            !floor.exists(),
            "an ungated record must not create the floor"
        );
        assert_eq!(wal.append(1, &scoped, true).unwrap(), 3);
        assert_eq!(std::fs::read_to_string(&floor).unwrap(), "scope/v1\n");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&floor);
    }

    /// Full replay, and the writer's own open, refuse a WAL holding a frame only a newer build
    /// can read, by name. Open must not treat it as a torn tail and truncate it away.
    #[test]
    fn full_replay_refuses_a_newer_writer_frame_by_name() {
        let path = tmp("newer-writer-replay");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
        }
        let offset = append_newer_writer_frame(&path, 2);
        let before = std::fs::read(&path).unwrap();

        expect_newer_writer(replay_file(&path, DEFAULT_MAX_READ_BYTES), offset);
        expect_newer_writer(
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None),
            offset,
        );
        expect_newer_writer(FileWal::open(&path).map(|_| ()), offset);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "no reader may cut it"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The anchored read behind `session.read` refuses a newer-writer frame at its anchor
    /// WITHOUT falling back to a read from byte zero, which it does for a corrupt frame there.
    /// A fallback would decode the whole prefix only to reach the same frame, so the decoded
    /// bytes must be exactly the suffix between the anchor and the refused frame (the lineage
    /// frame every append leaves behind its record).
    #[cfg(unix)]
    #[test]
    fn anchored_read_refuses_a_newer_writer_without_a_full_read() {
        let path = tmp("newer-writer-anchored");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 1..=2 {
            wal.append(1, &run_started(i), true).unwrap();
        }
        let anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(2), None)
            .unwrap()
            .next_anchor
            .unwrap();
        let offset = append_newer_writer_frame(&path, 3);
        let suffix_before_frame = offset - anchor.byte_end;
        assert_eq!(
            suffix_before_frame,
            crate::framing::LINEAGE_FRAME_LEN as u64
        );

        DECODED_BYTE_COUNT.with(|count| count.set(0));
        expect_newer_writer(
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 2, None, Some(&anchor)),
            offset,
        );
        assert_eq!(
            DECODED_BYTE_COUNT.with(std::cell::Cell::get),
            suffix_before_frame,
            "the anchored read fell back to decoding the WAL from byte zero"
        );

        // The strict variant, which the subscription stream uses to seed from the store,
        // answers with the refusal, not with `None`, which would mean "anchor unusable, take
        // the full-replay path".
        DECODED_BYTE_COUNT.with(|count| count.set(0));
        expect_newer_writer(
            read_records_range_from_valid_anchor(&path, DEFAULT_MAX_READ_BYTES, 2, None, &anchor),
            offset,
        );
        assert_eq!(
            DECODED_BYTE_COUNT.with(std::cell::Cell::get),
            suffix_before_frame
        );
        let _ = std::fs::remove_file(&path);
    }

    /// A newer-writer frame after the anchor's first record is refused by name too.
    #[cfg(unix)]
    #[test]
    fn anchored_read_refuses_a_later_newer_writer_frame() {
        let path = tmp("newer-writer-anchored-later");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 1..=2 {
            wal.append(1, &run_started(i), true).unwrap();
        }
        let anchor = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, Some(1), None)
            .unwrap()
            .next_anchor
            .unwrap();
        let offset = append_newer_writer_frame(&path, 3);
        expect_newer_writer(
            read_records_range(&path, DEFAULT_MAX_READ_BYTES, 1, None, Some(&anchor)),
            offset,
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Archive members decode through the same reader, so a folded newer-writer WAL is refused
    /// by name as well.
    #[test]
    fn archive_member_decode_refuses_a_newer_writer_by_name() {
        let path = tmp("newer-writer-member");
        let _ = std::fs::remove_file(&path);
        {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &run_started(1), true).unwrap();
        }
        let offset = append_newer_writer_frame(&path, 2);
        let bytes = std::fs::read(&path).unwrap();
        expect_newer_writer(read_verified_member_range(&bytes, 1, 0, None), offset);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn range_reader_never_truncates_a_torn_tail() {
        let path = tmp("range-reader-no-trunc");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(0), true).unwrap();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
            file.flush().unwrap();
        }
        let bytes_with_torn_tail = std::fs::read(&path).unwrap();

        let ranged = read_records_range(&path, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        assert_eq!(ranged.records.len(), 1);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes_with_torn_tail,
            "range readers discard torn bytes in memory without mutating the WAL"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The three states `replay_file` collapses into one, held apart.
    ///
    /// Non-vacuous by construction: the first two assertions BOTH pass through `replay_file`
    /// today -- absent and present-but-empty each return zero records -- so only the
    /// `wal_file_exists` column discriminates them. Without that function this test cannot be
    /// written at all.
    #[test]
    fn wal_file_exists_separates_absent_from_present_and_empty() {
        let absent = tmp("exists-absent");
        let empty = tmp("exists-empty");
        let full = tmp("exists-full");
        let _ = std::fs::remove_file(&absent);
        std::fs::write(&empty, b"").unwrap();

        let mut wal = FileWal::open(&full).unwrap();
        wal.append(1, &run_started(0), true).unwrap();

        // What replay_file sees: absent and empty are INDISTINGUISHABLE.
        assert_eq!(
            replay_file(&absent, DEFAULT_MAX_READ_BYTES).unwrap().len(),
            0
        );
        assert_eq!(
            replay_file(&empty, DEFAULT_MAX_READ_BYTES).unwrap().len(),
            0
        );
        assert_eq!(replay_file(&full, DEFAULT_MAX_READ_BYTES).unwrap().len(), 1);

        // What wal_file_exists adds: the column that was missing.
        assert!(
            !wal_file_exists(&absent).unwrap(),
            "absent file must report false"
        );
        assert!(
            wal_file_exists(&empty).unwrap(),
            "present empty file must report true"
        );
        assert!(
            wal_file_exists(&full).unwrap(),
            "present populated file must report true"
        );

        let _ = std::fs::remove_file(&empty);
        let _ = std::fs::remove_file(&full);
    }

    /// TRIPWIRE. `Partial` has NO producer today: the three constructions are driven by
    /// `file_present` being `Some(true)` / `Some(false)` / `None`, giving `Complete`,
    /// `Absent` and `Unknown`. Every consumer therefore writes `Complete | Partial | ...`
    /// and binds the records, which is correct precisely BECAUSE the variant cannot occur.
    ///
    /// The archive fold will make it occur — a member that decodes partially is exactly
    /// `Partial` — and at that moment those or-patterns start serving a TRUNCATED
    /// transcript as a complete one, silently, at every site that compiles unchanged.
    /// That is the silent-prefix failure an adversarial panel already caught once in the
    /// archive design, one layer down.
    ///
    /// So this test exists to fail when someone adds the first producer. If that is you:
    /// do not delete it. Revisit every `Complete | Partial` or-pattern first and decide,
    /// per site, whether a partial range is acceptable there — then update this test to
    /// say so. The compiler cannot ask you, because the arms already exist.
    #[test]
    fn partial_has_no_producer_and_making_one_must_revisit_every_consumer() {
        // SCAN THE PRODUCTION HALF ONLY. This test's own assertion text names the variant,
        // so scanning the whole file makes the test match itself and fail for a reason that
        // has nothing to do with the code -- the same self-matching defect a contributor
        // found in manifest.rs, where an assertion's literal satisfied its own search.
        let source = include_str!("file_wal.rs");
        let production = source
            .split_once("#[cfg(test)]\nmod tests")
            .expect("the test module marker must exist for this scan to be bounded")
            .0;
        let producers = production
            .lines()
            .filter(|line| {
                let line = line.trim_start();
                // A construction, not a match arm: `=> WalRangeRecords::Partial(...)` or a
                // bare `WalRangeRecords::Partial(...)` in expression position. Match arms
                // reach the variant through a leading `|` or the `matches!` macro.
                line.contains("WalRangeRecords::Partial")
                    && !line.starts_with('|')
                    && !line.starts_with("///")
                    && !line.contains("matches!")
            })
            .count();
        assert_eq!(
            producers, 0,
            "a WalRangeRecords::Partial producer appeared. Before changing this number, \
             read the comment above this test: every consumer currently lumps Partial \
             with Complete, and those sites compile unchanged while serving a truncated \
             transcript as a complete one."
        );
    }

    #[test]
    fn range_read_presence_separates_absent_from_present_and_empty() {
        let absent = tmp("range-presence-absent");
        let empty = tmp("range-presence-empty");
        let _ = std::fs::remove_file(&absent);
        std::fs::write(&empty, b"").unwrap();

        let absent_range =
            read_records_range(&absent, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();
        let empty_range =
            read_records_range(&empty, DEFAULT_MAX_READ_BYTES, 0, None, None).unwrap();

        assert_eq!(absent_range.records.len(), 0);
        assert_eq!(empty_range.records.len(), 0);
        assert_eq!(absent_range.file_present, Some(false));
        assert_eq!(empty_range.file_present, Some(true));

        let _ = std::fs::remove_file(&empty);
    }

    #[test]
    fn range_read_without_file_observation_keeps_presence_unknown() {
        let range = WalRangeRead::without_anchors(Vec::new());
        assert_eq!(range.file_present, None);
    }

    #[test]
    fn public_anchored_constructor_does_not_assert_presence() {
        // The public constructor takes in-memory records and opens nothing. If it
        // claimed `Some(true)`, every test double built on it would read as "the
        // file is there, skip the archive" — the failure that returns an empty
        // transcript for a session whose bytes sit in an archive nobody consulted.
        // Only `select_range`, reached after a real `File::open`, may set presence.
        let anchor = WalByteAnchor {
            file_id: 7,
            seq: 1,
            byte_end: 100,
        };
        let range = WalRangeRead::with_anchors(Vec::new(), vec![anchor]);
        assert_eq!(range.file_present, None);
        assert_eq!(range.next_anchor, Some(anchor));
    }

    #[test]
    fn replay_file_reader_never_truncates_a_torn_tail() {
        // THE CONCURRENT-READER INVARIANT: `replay_file` (the reader path, e.g. a
        // subscriber's attach replay) runs CONCURRENTLY with the single live writer. A
        // writer mid-`write_all` leaves a PARTIAL trailing frame that a reader sees as a
        // torn tail. The reader MUST ignore it in memory and leave the file BYTE-FOR-BYTE
        // unchanged — truncating it (as the writer-recovery `open` does) would cut the
        // writer's own in-progress bytes off the file and trip its single-writer integrity
        // check (ForeignWrite) on the next append, panicking the run. Regression for the
        // bug where the reader went through the truncating `open`.
        let path = tmp("reader-no-trunc");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(0), true).unwrap(); // one complete, fsynced record
                                                       // Simulate a writer mid-append: append PARTIAL frame bytes (a torn tail) directly.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            // Fewer bytes than a full frame — simulates a writer caught mid-append.
            f.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
            f.flush().unwrap();
        }
        let size_with_torn = std::fs::metadata(&path).unwrap().len();

        // The READER: returns only the valid record, and DOES NOT touch the file.
        let recs = replay_file(&path, DEFAULT_MAX_READ_BYTES).unwrap();
        assert_eq!(
            recs.len(),
            1,
            "reader returns the valid record, torn tail ignored"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            size_with_torn,
            "replay_file (reader) MUST NOT truncate — the file is unchanged"
        );

        // CONTRAST: `open` is the writer-recovery path and DOES truncate the torn tail
        // (so the next append starts clean). This documents the deliberate split.
        let _ = FileWal::open(&path).unwrap();
        assert!(
            std::fs::metadata(&path).unwrap().len() < size_with_torn,
            "FileWal::open (writer recovery) truncates the torn tail"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn preencoded_append_uses_encoder_bytes_for_precise_floats_and_present_nulls() {
        let path = tmp("preencoded-precise-null");
        let mut wal = FileWal::open(&path)
            .unwrap()
            .with_feature_floor(path.with_extension("floor"));
        let mut record = run_started(1);
        let WalRecord::ModelStepFinished { hooks, .. } = &mut record else {
            unreachable!()
        };
        *hooks = Some(Box::new(crate::record::Hooks {
            original: Some(serde_json::Value::Null),
            final_value: Some(serde_json::json!({"precise": 1.2345678901234567})),
            ops: Vec::new(),
            unavailable: None,
        }));
        let frame = encode(1, 7, &record).unwrap();
        assert!(frame
            .windows(b"1.2345678901234567".len())
            .any(|bytes| bytes == b"1.2345678901234567"));
        assert!(frame
            .windows(br#""original":null"#.len())
            .any(|bytes| bytes == br#""original":null"#));
        let before = std::fs::metadata(&path).unwrap().len() as usize;
        assert_eq!(
            wal.append_preencoded(7, &record, true, frame.clone())
                .unwrap(),
            1
        );
        let written = std::fs::read(&path).unwrap();
        assert_eq!(&written[before..before + frame.len()], frame.as_slice());

        let next = run_started(2);
        let pretty = serde_json::to_vec_pretty(&next).unwrap();
        let noncanonical = crate::framing::encode_payload(2, 7, &pretty).unwrap();
        assert!(
            wal.append_preencoded(7, &next, true, noncanonical).is_err(),
            "equal decoded values are not the encoder's measured bytes"
        );
        assert_eq!(std::fs::read(&path).unwrap(), written);
        assert_eq!(
            wal.append_preencoded(7, &next, true, encode(2, 7, &next).unwrap())
                .unwrap(),
            2
        );
    }

    #[test]
    fn preencoded_append_reuses_the_measured_frame_bytes() {
        let path = tmp("preencoded");
        let record = run_started(1);
        let frame = encode(1, 1, &record).unwrap();
        let mut wal = FileWal::open(&path).unwrap();
        let before = std::fs::metadata(&path).unwrap().len() as usize;
        wal.append_preencoded(1, &record, true, frame.clone())
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[before..before + frame.len()], frame.as_slice());
        assert_eq!(wal.replay().unwrap()[0].record, record);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn replay_file_refuses_oversized_wal_without_mutating_it() {
        let path = tmp("too-large");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(0), true).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let len_before = bytes.len() as u64;

        let err = replay_file(&path, len_before.saturating_sub(1)).expect_err("cap trips");
        assert!(
            matches!(
                err,
                WalError::TooLarge {
                    max_bytes,
                    file_size
                } if max_bytes == len_before.saturating_sub(1) && file_size == len_before
            ),
            "got {err:?}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "size-cap rejection must leave the WAL bytes untouched"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn many_appends_on_one_handle_all_accept() {
        // The single-writer happy path: N sequential appends on ONE handle all succeed
        // (the integrity check tracks our own writes — size always matches).
        let path = tmp("single-writer");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        for i in 0..20 {
            wal.append(1, &run_started(i), i % 3 == 0).unwrap();
        }
        assert_eq!(wal.replay().unwrap().len(), 20);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn foreign_append_via_second_handle_is_rejected() {
        // The real late-drain interleave: a SECOND FileWal handle (a foreign writer) on
        // the same path appends a record; the FIRST handle's next append must be REJECTED
        // (it fstat-observes the file grew under it via the other handle).
        let path = tmp("foreign");
        let _ = std::fs::remove_file(&path);
        let mut w_old = FileWal::open(&path).unwrap();
        w_old.append(1, &run_started(0), true).unwrap();

        // A foreign writer (new handle) appends — the inode's size grows.
        {
            let mut w_new = FileWal::open(&path).unwrap();
            w_new.append(2, &run_started(1), true).unwrap();
        }

        // w_old's next append sees the file grew under it → ForeignWrite, no write.
        let err = w_old
            .append(1, &run_started(2), true)
            .expect_err("a foreign append must be rejected");
        assert!(
            matches!(err, WalError::ForeignWrite { .. }),
            "got {err:?}, expected ForeignWrite"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn truncation_under_the_writer_is_rejected() {
        // A size DECREASE (truncation of an append-only file) is equally anomalous → the
        // != check rejects it too.
        let path = tmp("trunc");
        let _ = std::fs::remove_file(&path);
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &run_started(0), true).unwrap();
        wal.append(1, &run_started(1), true).unwrap();

        // Truncate the file out from under the writer.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(0).unwrap();
        }
        let err = wal
            .append(1, &run_started(2), true)
            .expect_err("a truncation must be rejected");
        assert!(matches!(err, WalError::ForeignWrite { .. }), "got {err:?}");
        let _ = std::fs::remove_file(&path);
    }
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, broaden, deny_acl_read, observe, reset_acl,
        BroadParent,
    };
    use std::rc::Rc;

    #[test]
    fn windows_wal_manifest_and_archive_creation_is_private() {
        let fixture = BroadParent::new("wal");
        let path = fixture.path.join("nested/session.wal");
        let manifest = fixture.path.join("wal-manifest");
        let seen = Rc::new(RefCell::new(Vec::new()));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.borrow_mut().push(path.to_path_buf());
            },
            || {
                FileWal::open_with_manifest(
                    &path,
                    &manifest,
                    "abc",
                    "root\u{1f}harness\u{1f}session",
                )
                .unwrap();
                crate::archive::ArchiveWriter::create(fixture.path.join("fold-1.ark")).unwrap();
            },
        );
        for candidate in [
            &path,
            &manifest,
            &fixture.path.join("fold-1.ark"),
            &fixture.path.join("nested"),
        ] {
            assert_private(candidate);
        }
        assert!(seen.borrow().contains(&path));
        assert!(seen.borrow().contains(&manifest));
    }

    fn refuses_before_replay(manifest_enabled: bool) {
        let fixture = BroadParent::new("wal-refusal");
        let path = fixture.path.join("session.wal");
        FileWal::open(&path).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"torn tail")
            .unwrap();
        let original = std::fs::read(&path).unwrap();
        for unreadable in [false, true] {
            if unreadable {
                deny_acl_read(&path);
            } else {
                broaden(&path);
            }
            OPEN_STAGES.with(|stages| stages.borrow_mut().clear());
            let manifest = fixture.path.join("wal-manifest");
            let result = if manifest_enabled {
                FileWal::open_with_manifest(
                    &path,
                    &manifest,
                    "abc",
                    "root\u{1f}harness\u{1f}session",
                )
            } else {
                FileWal::open(&path)
            };
            assert!(result.is_err(), "non-private WAL was opened");
            OPEN_STAGES.with(|stages| {
                assert!(
                    stages.borrow().is_empty(),
                    "WAL IO reached after ACL refusal: {:?}",
                    stages.borrow()
                )
            });
            assert!(!manifest.exists());
            reset_acl(&path);
            assert_eq!(
                std::fs::read(&path).unwrap(),
                original,
                "refusal altered the torn tail"
            );
            broca_owner_only::repair(&path).unwrap();
        }
    }

    #[test]
    fn windows_existing_wal_refused_before_replay() {
        refuses_before_replay(false);
    }
    #[test]
    fn windows_manifest_wal_refused_before_replay() {
        refuses_before_replay(true);
    }
}
