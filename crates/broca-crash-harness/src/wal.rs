//! Reading and cutting the session log at frame boundaries, and observing where the runner's
//! durability barriers fall.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use broca_core::{FileWalStore, WalStore};
use broca_protocol::BindIdentity;
use broca_wal::framing::{decode_next, DecodeStep};
use broca_wal::{FramedRecord, Wal, WalError, WalLineageId, WalRecord};

/// One complete frame of a session log, with its byte range in the file.
#[derive(Debug, Clone)]
pub struct Frame {
    pub start: usize,
    pub end: usize,
    pub body: FrameBody,
}

#[derive(Debug, Clone)]
pub enum FrameBody {
    /// The lineage identity frame a new log starts with.
    Lineage(WalLineageId),
    Record(Box<FramedRecord>),
}

/// A session log decoded frame by frame, without the recovery a writer open performs, so
/// reading it never changes the file.
#[derive(Debug, Clone, Default)]
pub struct LogImage {
    pub bytes: Vec<u8>,
    pub frames: Vec<Frame>,
    /// Bytes after the last complete frame: an append a crash cut short.
    pub torn_tail: usize,
}

impl LogImage {
    /// Decode `bytes`. A crash can only cut a log short, leaving an incomplete last frame,
    /// which is reported as `torn_tail`. It cannot turn a complete frame into one that fails
    /// its digest, and this build wrote every frame the harness reads, so a frame needing a
    /// newer reader cannot appear either. Both are harness failures.
    pub fn parse(bytes: Vec<u8>) -> Self {
        let mut frames = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            match decode_next(&bytes[at..]) {
                DecodeStep::Record { framed, consumed } => {
                    frames.push(Frame {
                        start: at,
                        end: at + consumed,
                        body: FrameBody::Record(framed),
                    });
                    at += consumed;
                }
                DecodeStep::Lineage {
                    lineage_id,
                    consumed,
                } => {
                    frames.push(Frame {
                        start: at,
                        end: at + consumed,
                        body: FrameBody::Lineage(lineage_id),
                    });
                    at += consumed;
                }
                DecodeStep::TornTail => break,
                DecodeStep::Corrupt(error) => {
                    panic!("session log is corrupt at byte {at}: {error}")
                }
                DecodeStep::NewerWriter { ver, missing } => {
                    panic!("session log holds a frame ver {ver} needing {missing:?}")
                }
            }
        }
        let torn_tail = bytes.len() - at;
        Self {
            bytes,
            frames,
            torn_tail,
        }
    }

    /// Read the log at `path`; an absent file reads as empty.
    pub fn read(path: &Path) -> Self {
        match std::fs::read(path) {
            Ok(bytes) => Self::parse(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(error) => panic!("reading session log {}: {error}", path.display()),
        }
    }

    pub fn lineage_id(&self) -> Option<WalLineageId> {
        self.frames.iter().find_map(|frame| match frame.body {
            FrameBody::Lineage(id) => Some(id),
            FrameBody::Record(_) => None,
        })
    }

    /// Every durable record, in log order.
    pub fn framed(&self) -> Vec<FramedRecord> {
        self.frames
            .iter()
            .filter_map(|frame| match &frame.body {
                FrameBody::Record(framed) => Some(framed.as_ref().clone()),
                FrameBody::Lineage(_) => None,
            })
            .collect()
    }

    pub fn records(&self) -> Vec<WalRecord> {
        self.framed()
            .into_iter()
            .map(|framed| framed.record)
            .collect()
    }

    pub fn record_count(&self) -> usize {
        self.frames
            .iter()
            .filter(|frame| matches!(frame.body, FrameBody::Record(_)))
            .count()
    }

    /// The byte offset just after the first `records` record frames, with every frame that
    /// precedes them (the lineage frame) kept.
    pub fn end_of_records(&self, records: usize) -> usize {
        if records == 0 {
            return self
                .frames
                .iter()
                .take_while(|frame| matches!(frame.body, FrameBody::Lineage(_)))
                .last()
                .map_or(0, |frame| frame.end);
        }
        self.frames
            .iter()
            .filter(|frame| matches!(frame.body, FrameBody::Record(_)))
            .nth(records - 1)
            .unwrap_or_else(|| panic!("the log has fewer than {records} records"))
            .end
    }

    /// The frame of the `index`-th record (0-based).
    pub fn record_frame(&self, index: usize) -> Option<&Frame> {
        self.frames
            .iter()
            .filter(|frame| matches!(frame.body, FrameBody::Record(_)))
            .nth(index)
    }
}

/// Where a session's log lives under a state root.
pub fn wal_path(state_root: &Path, session: &BindIdentity) -> PathBuf {
    FileWalStore::new(state_root.join("wal")).wal_path(session)
}

/// A [`FileWalStore`] that records the sequence number the log reached at every completed
/// durability barrier. The runner cannot tell it from the plain store; the harness uses the
/// barriers to say, for each record boundary, whether a crash there falls just after a sync
/// or inside a batch of appends that had not been synced yet.
pub struct ObservedWalStore {
    inner: FileWalStore,
    synced: Arc<Mutex<BTreeSet<u64>>>,
}

impl ObservedWalStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            inner: FileWalStore::new(root),
            synced: Arc::default(),
        }
    }

    /// The sequence numbers the log had reached when each barrier completed.
    pub fn synced(&self) -> BTreeSet<u64> {
        self.synced.lock().unwrap().clone()
    }
}

// EVERY METHOD FileWalStore OVERRIDES IS FORWARDED HERE. A method left to the trait default
// would make the reference run take a different path from production: the defaults refuse
// retention, ignore the session class, and know no tombstone floors. The
// test in tests/it/observed_wal_fidelity.rs compares these impls with FileWalStore's and
// FileWal's, so a new production override cannot be missed here silently.
impl WalStore for ObservedWalStore {
    fn uploads(&self) -> Option<broca_core::media_uploads::Uploads> {
        WalStore::uploads(&self.inner)
    }

    fn read_records_tail_bounded(
        &self,
        session: &BindIdentity,
        tail_bytes: u64,
    ) -> Result<Option<broca_wal::WalRangeRead>, WalError> {
        WalStore::read_records_tail_bounded(&self.inner, session, tail_bytes)
    }

    fn backup_classes(&self) -> Vec<String> {
        WalStore::backup_classes(&self.inner)
    }

    fn retired_tag_floor(&self, session: &BindIdentity) -> Result<u64, WalError> {
        WalStore::retired_tag_floor(&self.inner, session)
    }

    fn retired_notice_floor(&self, session: &BindIdentity) -> Result<u64, WalError> {
        WalStore::retired_notice_floor(&self.inner, session)
    }

    fn open_with_class(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(self.observe(WalStore::open_with_class(
            &self.inner,
            session,
            retention,
            session_class,
        )?))
    }

    fn open_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        lease: Box<dyn broca_core::LeaseHandle>,
    ) -> Result<(Box<dyn Wal + Send>, Box<dyn broca_core::LeaseHandle>), WalError> {
        let (wal, lease) =
            WalStore::open_writer(&self.inner, session, retention, session_class, lease)?;
        Ok((self.observe(wal), lease))
    }

    fn open_owned_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        session_class: Option<&str>,
        lease: Box<dyn broca_core::LeaseHandle>,
    ) -> Result<(Box<dyn Wal + Send>, Box<dyn broca_core::LeaseHandle>), WalError> {
        let (wal, lease) =
            WalStore::open_owned_writer(&self.inner, session, retention, session_class, lease)?;
        Ok((self.observe(wal), lease))
    }

    fn check_move(&self, session: &BindIdentity, write: bool) -> Result<(), WalError> {
        WalStore::check_move(&self.inner, session, write)
    }

    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(self.observe(WalStore::open(&self.inner, session)?))
    }

    fn open_with_retention(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
    ) -> Result<Box<dyn Wal + Send>, WalError> {
        Ok(self.observe(WalStore::open_with_retention(
            &self.inner,
            session,
            retention,
        )?))
    }

    fn lineage_exists(
        &self,
        session: &BindIdentity,
        meta: Option<&broca_core::AdmissionStoreSnapshot>,
    ) -> Result<bool, WalError> {
        WalStore::lineage_exists(&self.inner, session, meta)
    }

    fn first_user_message(
        &self,
        session: &BindIdentity,
    ) -> Result<(Option<broca_protocol::Message>, u64), WalError> {
        WalStore::first_user_message(&self.inner, session)
    }

    fn is_retention_lineage(&self, session: &BindIdentity) -> Result<bool, WalError> {
        WalStore::is_retention_lineage(&self.inner, session)
    }

    fn wal_present(&self, session: &BindIdentity) -> Option<bool> {
        WalStore::wal_present(&self.inner, session)
    }

    fn live_wal_last_write(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<std::time::SystemTime>, WalError> {
        WalStore::live_wal_last_write(&self.inner, session)
    }

    fn live_wal_bytes(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        WalStore::live_wal_bytes(&self.inner, session)
    }

    fn read_records(&self, session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        WalStore::read_records(&self.inner, session)
    }

    fn read_records_with_lineage(
        &self,
        session: &BindIdentity,
    ) -> Result<(Vec<FramedRecord>, Option<WalLineageId>), WalError> {
        WalStore::read_records_with_lineage(&self.inner, session)
    }

    fn read_records_range(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, WalError> {
        WalStore::read_records_range(&self.inner, session, after_seq, through_seq, anchor)
    }

    fn read_records_range_for_authorization(
        &self,
        session: &BindIdentity,
        after_seq: u64,
        through_seq: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, WalError> {
        WalStore::read_records_range_for_authorization(
            &self.inner,
            session,
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
        anchor: &broca_wal::WalByteAnchor,
    ) -> Result<Option<broca_wal::WalRangeRead>, WalError> {
        WalStore::read_records_range_from_valid_anchor(
            &self.inner,
            session,
            after_seq,
            through_seq,
            anchor,
        )
    }

    fn flush_for_read(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        WalStore::flush_for_read(&self.inner, session)
    }

    fn file_identity(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        WalStore::file_identity(&self.inner, session)
    }

    fn flush_for_authorization(&self, session: &BindIdentity) -> Result<Option<u64>, WalError> {
        WalStore::flush_for_authorization(&self.inner, session)
    }

    fn file_identity_for_authorization(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<u64>, WalError> {
        WalStore::file_identity_for_authorization(&self.inner, session)
    }
}

impl ObservedWalStore {
    fn observe(&self, inner: Box<dyn Wal + Send>) -> Box<dyn Wal + Send> {
        Box::new(ObservedWal {
            inner,
            synced: Arc::clone(&self.synced),
        })
    }
}

struct ObservedWal {
    inner: Box<dyn Wal + Send>,
    synced: Arc<Mutex<BTreeSet<u64>>>,
}

// Like the store above, every method FileWal overrides is forwarded, so the writer sees the
// same byte budget and takes the same pre-encoded append path as in production.
impl Wal for ObservedWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let seq = self.inner.append(fence, record, sync)?;
        if sync {
            self.synced.lock().unwrap().insert(seq);
        }
        Ok(seq)
    }

    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        let seq = self.inner.append_preencoded(fence, record, sync, frame)?;
        if sync {
            self.synced.lock().unwrap().insert(seq);
        }
        Ok(seq)
    }

    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        self.inner.append_budget()
    }

    fn recovery_byte_cap(&self) -> Option<u64> {
        self.inner.recovery_byte_cap()
    }

    fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
        self.inner.append_budget_scope()
    }

    fn sync(&self) -> Result<(), WalError> {
        self.inner.sync()?;
        self.synced.lock().unwrap().insert(self.inner.last_seq());
        Ok(())
    }

    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.replay()
    }

    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        self.inner.tail_after_seq(after_seq)
    }

    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }

    fn lineage_id(&self) -> Option<WalLineageId> {
        self.inner.lineage_id()
    }
}
