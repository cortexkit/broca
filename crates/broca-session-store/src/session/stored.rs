//! Reading a session's stored records apart from its writer.
//!
//! A subscriber catching up on a session reads records it hasn't seen while the session's writer
//! goes on appending. It reads them through a [`StoredRecords`]: where the records were stored
//! when it attached (the head as the writer last committed it, and the open log), over a backend
//! handle of its own, so its reads never wait on the writer and the writer never waits on them.
//! Records appended after it attached are past the end it reads to, and a torn tail of the open
//! log reads as not yet written.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::backend::{BackendError, SessionAddr, SessionBackend};
use crate::checkpoint::Checkpoint;
use crate::codec::{Id128, Message as _};
use crate::directory::Pages;
use crate::fold::conversation::Conversation;
use crate::frame::{FrameHeader, FrameKey, FrameRead, FrameStream};
use crate::head::Head;
use crate::idindex::{self, IdKind, IndexPages, IndexRoot};
use crate::keys::DataKey;
use crate::log::{LogReader, ReadLog};
use crate::record::{Record, RunStart, Stream};
use crate::segment::{SegmentLayout, FOOTER, OPEN_PREFIX};

use super::{chain_base, log_bind, Session, SessionError};

/// Where a session's records were stored at one moment: the head as its writer last committed
/// it, and the open log.
pub(super) struct Place<'a, B> {
    pub(super) backend: &'a B,
    pub(super) addr: &'a SessionAddr,
    pub(super) head: &'a Head,
    pub(super) key: &'a DataKey,
    pub(super) log: &'a Id128,
    /// What reads through this place keep for the reads after them, when its owner keeps
    /// anything (see [`Kept`]).
    pub(super) kept: Option<&'a Kept>,
}

/// What a [`StoredRecords`] keeps between its reads, so a reader that pages through a session,
/// as a subscriber's catch-up does, reads the open log whole once and each sealed segment's
/// layout once, not once per page.
#[derive(Default)]
pub(super) struct Kept {
    /// Where the open log's records lie, once a read has read the log whole.
    open: Mutex<Option<Arc<OpenMap>>>,
    /// The layouts of the sealed segments read most recently, by object name, oldest first, at
    /// most [`LAYOUTS_KEPT`].
    layouts: Mutex<Vec<(String, Arc<SegmentLayout>)>>,
}

/// How many sealed segments' layouts a [`Kept`] holds: a reader pages through a few segments at
/// a time.
const LAYOUTS_KEPT: usize = 32;

/// The open log as a whole read of it found it: its checkpoint, and where each of its complete
/// batches starts, so a later read of a range of its records reads only the batches that hold
/// them. A log only grows past its complete batches, so where they lie never changes.
pub(super) struct OpenMap {
    checkpoint: Checkpoint,
    /// The seq of each complete batch's first record and the offset the batch starts at, in seq
    /// order.
    batches: Vec<(u64, u64)>,
    /// Where the complete batches end.
    end: u64,
}

/// The records a read asks for: those from seq `first` through seq `last`, but for ledger records
/// before `ledger_from`.
#[derive(Clone, Copy)]
pub(super) struct Want {
    first: u64,
    last: u64,
    ledger_from: u64,
}

impl Want {
    /// Every record from `first` through `last`.
    fn between(first: u64, last: u64) -> Self {
        Self {
            first,
            last,
            ledger_from: 0,
        }
    }

    /// The records a replay of the log that started from `checkpoint` folds from `first`
    /// through `last`: its ledger records the checkpoint covers are folded into it already.
    fn replay(checkpoint: &Checkpoint, first: u64, last: u64) -> Self {
        Self {
            first,
            last,
            ledger_from: checkpoint.covers_through.saturating_add(1),
        }
    }

    fn holds(&self, seq: u64, stream: Stream) -> bool {
        (self.first..=self.last).contains(&seq)
            && (seq >= self.ledger_from || stream == Stream::Transcript)
    }
}

impl<B: SessionBackend> Place<'_, B> {
    /// The open log read back: its checkpoint and its records. The writer appended every one of
    /// them, so a log that no longer reads clean means something else wrote it, and the read
    /// refuses.
    pub(super) async fn open_log(&self) -> Result<ReadLog, SessionError> {
        let open =
            self.head
                .open_log
                .clone()
                .ok_or(SessionError::Backend(BackendError::Corrupt(
                    "local head without a log",
                )))?;
        let end = self.backend.log_end(self.addr, self.log).await?;
        let bytes = self.backend.read_log(self.addr, self.log, 0..end).await?;
        let bind = log_bind(&self.head.session_id, self.log);
        match crate::damage::analyse(
            &bytes,
            &self.key.frame_key(),
            &bind,
            open.synced_len,
            chain_base,
            None,
        )? {
            crate::damage::Verdict::Opens(read) => Ok(read),
            _ => Err(SessionError::Fold(
                "the open log no longer reads clean under its writer".into(),
            )),
        }
    }

    /// Every record of both streams from seq `first` through seq `last`, sealed or in the open
    /// log, in seq order, reading only the sealed blocks that overlap the range. A record a seal
    /// kept in the open log is read once.
    pub(super) async fn records_between(
        &self,
        first: u64,
        last: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.wanted(Want::between(first, last), None).await
    }

    /// The records `want` asks for, sealed or in the open log, in seq order. The open log's are
    /// read through `open` when the caller has its map at hand, else as [`Self::open_records`]
    /// reads them.
    async fn wanted(
        &self,
        want: Want,
        open: Option<&OpenMap>,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        let mut records = if self.sealed_reach(want.first) {
            self.sealed_between(want).await?
        } else {
            BTreeMap::new()
        };
        let open = match open {
            Some(open) => open.read(self, want).await?,
            None => self.open_records(|_| Some(want)).await?.1,
        };
        records.extend(open);
        Ok(records.into_iter().collect())
    }

    /// The open log's map, and the records of the log that `want` asks for once it has seen the
    /// log's checkpoint (none when it asks for none). With a map kept from an earlier read, this
    /// reads only the batches that hold those records, and nothing when the log holds none of
    /// them. Otherwise it reads the log whole, batch by batch, keeping only the wanted records,
    /// never the whole decoded log, and keeps the map when the place keeps anything.
    async fn open_records(
        &self,
        want: impl FnOnce(&Checkpoint) -> Option<Want>,
    ) -> Result<(Arc<OpenMap>, Vec<(u64, Record)>), SessionError> {
        let kept = self
            .kept
            .and_then(|kept| kept.open.lock().expect("kept open log").clone());
        if let Some(map) = kept {
            let records = match want(&map.checkpoint) {
                Some(want) => map.read(self, want).await?,
                None => Vec::new(),
            };
            return Ok((map, records));
        }
        let (map, records) = self.scan_open(want).await?;
        let map = Arc::new(map);
        if let Some(kept) = self.kept {
            *kept.open.lock().expect("kept open log") = Some(Arc::clone(&map));
        }
        Ok((map, records))
    }

    /// Reads the open log whole with the checks [`Self::open_log`] makes, a batch at a time:
    /// maps where its batches lie, and keeps the records `want` asks for.
    async fn scan_open(
        &self,
        want: impl FnOnce(&Checkpoint) -> Option<Want>,
    ) -> Result<(OpenMap, Vec<(u64, Record)>), SessionError> {
        let open =
            self.head
                .open_log
                .clone()
                .ok_or(SessionError::Backend(BackendError::Corrupt(
                    "local head without a log",
                )))?;
        let end = self.backend.log_end(self.addr, self.log).await?;
        let bytes = self.backend.read_log(self.addr, self.log, 0..end).await?;
        let key = self.key.frame_key();
        let bind = log_bind(&self.head.session_id, self.log);
        let mut reader = LogReader::new(&bytes, &key, &bind, open.synced_len, chain_base);
        let mut want = Some(want);
        let mut wanted = None;
        let mut checkpoint = None;
        let mut batches = Vec::new();
        let mut records = Vec::new();
        while let Some(batch) = reader.next_batch()? {
            if checkpoint.is_none() {
                let start =
                    Checkpoint::decode(reader.checkpoint()).map_err(SessionError::Checkpoint)?;
                wanted = want.take().and_then(|want| want(&start));
                checkpoint = Some(start);
            }
            if let Some((first, _)) = batch.records.first() {
                batches.push((*first, batch.at));
            }
            if let Some(wanted) = wanted {
                records.extend(
                    batch
                        .records
                        .into_iter()
                        .filter(|(seq, record)| wanted.holds(*seq, record.stream())),
                );
            }
        }
        let read = match crate::damage::verdict(
            reader.finish(),
            &bytes,
            &key,
            &bind,
            open.synced_len,
            chain_base,
            None,
        )? {
            crate::damage::Verdict::Opens(read) => read,
            _ => {
                return Err(SessionError::Fold(
                    "the open log no longer reads clean under its writer".into(),
                ))
            }
        };
        let checkpoint = match checkpoint {
            Some(checkpoint) => checkpoint,
            // A log of only its checkpoint: no batch handed it on.
            None => Checkpoint::decode(&read.checkpoint).map_err(SessionError::Checkpoint)?,
        };
        let map = OpenMap {
            checkpoint,
            batches,
            end: read.complete_through,
        };
        Ok((map, records))
    }

    /// Whether a sealed segment may hold a record at or past `first`.
    pub(super) fn sealed_reach(&self, first: u64) -> bool {
        [&self.head.ledger_dir, &self.head.transcript_dir]
            .into_iter()
            .any(|dir| dir.as_ref().is_some_and(|root| root.last_seq >= first))
    }

    pub(super) async fn find_indexed_run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<RunStart>, SessionError> {
        let mut start = None;
        self.visit_indexed(IdKind::RunId, run_id, |seq, kept, record| {
            if !kept || record.stream() != Stream::Ledger {
                return Err(SessionError::Fold(format!("no sealed ledger record {seq}")));
            }
            if let Record::RunStart(candidate) = record {
                if candidate.run_id == run_id && start.is_none() {
                    start = Some((**candidate).clone());
                }
            }
            Ok(())
        })
        .await?;
        Ok(start)
    }

    pub(super) async fn has_indexed_call_intent(
        &self,
        call_key: &str,
    ) -> Result<bool, SessionError> {
        let mut found = false;
        self.visit_indexed(IdKind::CallKey, call_key, |_, _, record| {
            // Like Session::find_id, verify the named id, even on a hash match.
            found |= matches!(record, Record::ToolIntent(_))
                && idindex::names(record, IdKind::CallKey, call_key);
            Ok(())
        })
        .await?;
        Ok(found)
    }

    /// Read only the sealed blocks the index names. A dangling index entry is damage, so visit
    /// every candidate even if an earlier one matched the requested ID.
    async fn visit_indexed(
        &self,
        kind: IdKind,
        id: &str,
        mut visit: impl FnMut(u64, bool, &Record) -> Result<(), SessionError>,
    ) -> Result<(), SessionError> {
        let Some(root) = &self.head.id_index else {
            return Ok(());
        };
        let key = self.key.frame_key();
        let index = IndexPages {
            backend: self.backend,
            addr: self.addr,
            session: self.head.session_id,
            key: &key,
        };
        let root: IndexRoot = index.read(root).await.map_err(SessionError::Index)?;
        let hash = idindex::id_hash(&idindex::hash_key(self.key), kind, id);
        let seqs = index
            .lookup(&root, &hash)
            .await
            .map_err(SessionError::Index)?;
        let pages = Pages {
            backend: self.backend,
            addr: self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut memo = Default::default();
        let mut last_object = String::new();
        let mut last_block = None;
        let mut records = Vec::new();
        for seq in seqs {
            let damaged = || SessionError::Fold(format!("no sealed ledger record {seq}"));
            let entry = pages
                .find_in(self.head.ledger_dir.as_ref(), seq, &mut memo)
                .await
                .map_err(SessionError::Directory)?
                .ok_or_else(damaged)?;
            let layout = self.layout(&entry.object, &key).await?;
            let block = layout.block_for_seq(seq).ok_or_else(damaged)?;
            if last_block != Some(block) || last_object != entry.object {
                records = read_block(self.backend, self.addr, &entry.object, &layout, block, &key)
                    .await?;
                last_object.clone_from(&entry.object);
                last_block = Some(block);
            }
            let (_, record) = records
                .iter()
                .find(|(at, _)| *at == seq)
                .ok_or_else(damaged)?;
            visit(seq, entry.keeps(seq), record)?;
        }
        Ok(())
    }

    /// The layout of sealed segment `object` (see [`read_layout`]), from those the place keeps
    /// when it keeps one. A missing segment is damage: the directory names it.
    async fn layout(
        &self,
        object: &str,
        key: &FrameKey,
    ) -> Result<Arc<SegmentLayout>, SessionError> {
        let kept = self.kept.and_then(|kept| {
            let layouts = kept.layouts.lock().expect("kept layouts");
            layouts
                .iter()
                .find(|(name, _)| name == object)
                .map(|(_, layout)| Arc::clone(layout))
        });
        if let Some(layout) = kept {
            return Ok(layout);
        }
        let layout = read_layout(self.backend, self.addr, object, key)
            .await?
            .ok_or_else(|| {
                SessionError::Fold(format!("the directory names a missing segment {object}"))
            })?;
        let layout = Arc::new(layout);
        if let Some(kept) = self.kept {
            let mut layouts = kept.layouts.lock().expect("kept layouts");
            if layouts.len() == LAYOUTS_KEPT {
                layouts.remove(0);
            }
            layouts.push((object.to_owned(), Arc::clone(&layout)));
        }
        Ok(layout)
    }

    /// The sealed records `want` asks for, by seq, reading only the directory pages, segment
    /// layouts and blocks that overlap its range.
    async fn sealed_between(&self, want: Want) -> Result<BTreeMap<u64, Record>, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut records = BTreeMap::new();
        let streams = [
            (&self.head.ledger_dir, want.first.max(want.ledger_from)),
            (&self.head.transcript_dir, want.first),
        ];
        for (dir, first) in streams {
            let last = want.last;
            if first > last || dir.as_ref().is_none_or(|root| root.last_seq < first) {
                continue;
            }
            for entry in pages
                .entries_between(dir.as_ref(), first, last)
                .await
                .map_err(SessionError::Directory)?
            {
                let layout = self.layout(&entry.object, &key).await?;
                let blocks = &layout.index.blocks;
                for position in 0..blocks.len() {
                    // A block whose successor starts at or before `first` ends before it, and a
                    // block that starts after `last` holds nothing in the range.
                    if blocks
                        .get(position + 1)
                        .is_some_and(|next| next.first_seq <= first)
                        || blocks[position].first_seq > last
                    {
                        continue;
                    }
                    for (seq, record) in read_block(
                        self.backend,
                        self.addr,
                        &entry.object,
                        &layout,
                        position,
                        &key,
                    )
                    .await?
                    {
                        if (first..=last).contains(&seq) && entry.keeps(seq) {
                            records.insert(seq, record);
                        }
                    }
                }
            }
        }
        Ok(records)
    }

    /// The checkpoint of the sealed log holding `seq`: the newest log start that covers less
    /// than `seq`. Each ledger segment carries the start of the log after the one it sealed, in
    /// its index; the session's first log started from empty state.
    async fn sealed_log_start(&self, seq: u64) -> Result<Checkpoint, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: crate::directory::FANOUT,
        };
        let mut before = seq.saturating_add(1);
        loop {
            let Some(entry) = pages
                .find_before(self.head.ledger_dir.as_ref(), before)
                .await
                .map_err(SessionError::Directory)?
            else {
                return Ok(first_log_checkpoint(self.head));
            };
            let layout = read_layout(self.backend, self.addr, &entry.object, &key)
                .await?
                .ok_or(SessionError::CheckpointLost)?;
            let start = layout
                .index
                .log_start
                .as_ref()
                .ok_or(SessionError::CheckpointLost)?;
            let checkpoint = Checkpoint::decode(&start.0).map_err(SessionError::Checkpoint)?;
            if checkpoint.covers_through < seq {
                return Ok(checkpoint);
            }
            before = entry.first_seq;
        }
    }
}

impl OpenMap {
    /// The open log's records `want` asks for, read from the batches that can hold them in one
    /// ranged read of the log: nothing at all when the log's records all come after them.
    async fn read<B: SessionBackend>(
        &self,
        place: &Place<'_, B>,
        want: Want,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        // The batches from the last one starting at or before `first` (the first batch when none
        // does) up to the last one starting at or before `last`.
        let to = self
            .batches
            .partition_point(|(first, _)| *first <= want.last);
        if want.first > want.last || to == 0 {
            return Ok(Vec::new());
        }
        let from = self
            .batches
            .partition_point(|(first, _)| *first <= want.first)
            .saturating_sub(1);
        let start = self.batches[from].1;
        let end = self.batches.get(to).map_or(self.end, |(_, at)| *at);
        let bytes = place
            .backend
            .read_log(place.addr, place.log, start..end)
            .await?;
        let key = place.key.frame_key();
        let bind = log_bind(&place.head.session_id, place.log);
        let mut records = Vec::new();
        let mut previous = None;
        read_frames(&bytes, start, &key, &bind, |at, header, body| {
            // Seqs grow frame by frame, as the whole read of the log checked.
            if previous.is_some_and(|previous| header.seq <= previous) {
                return Err(SessionError::Fold(format!(
                    "the open log's frame at {at} holds record {}, out of order",
                    header.seq
                )));
            }
            previous = Some(header.seq);
            if header.seq > want.last {
                return Ok(false);
            }
            if want.holds(header.seq, frame_stream(header.stream)) {
                records.push((header.seq, decode_frame(at, header, &body)?));
            }
            Ok(true)
        })?;
        Ok(records)
    }
}

/// The stream a record frame of `stream` belongs to.
fn frame_stream(stream: FrameStream) -> Stream {
    match stream {
        FrameStream::Transcript => Stream::Transcript,
        _ => Stream::Ledger,
    }
}

/// Walks the frames of `bytes`, which were read from the open log at offset `start` and begin on
/// a frame, handing `each` the offset, header and body of every frame that carries a record
/// until it returns `false`. The writer appended every frame there, so one that doesn't read back
/// is damage.
pub(super) fn read_frames(
    bytes: &[u8],
    start: u64,
    key: &FrameKey,
    bind: &[u8],
    mut each: impl FnMut(u64, &FrameHeader, Vec<u8>) -> Result<bool, SessionError>,
) -> Result<(), SessionError> {
    let mut offset = 0;
    while offset < bytes.len() {
        let at = start + offset as u64;
        let (header, body, len) = match crate::frame::open(
            &bytes[offset..],
            |id| (id == key.id()).then_some(key),
            bind,
        ) {
            Ok(FrameRead::Frame { header, body, len }) => (header, body, len),
            Ok(FrameRead::Incomplete) => {
                return Err(SessionError::Fold(format!(
                    "the open log's frame at {at} is cut short"
                )))
            }
            Err(error) => {
                return Err(SessionError::Damaged(crate::log::LogEnd::Damaged {
                    at,
                    damage: crate::log::Damage::Frame(error),
                }))
            }
        };
        offset += len;
        // The checkpoint and claim frames carry no record.
        if header.stream == FrameStream::None {
            continue;
        }
        if !each(at, &header, body)? {
            break;
        }
    }
    Ok(())
}

/// The record the frame at `at` with `header` and `body` carries.
pub(super) fn decode_frame(
    at: u64,
    header: &FrameHeader,
    body: &[u8],
) -> Result<Record, SessionError> {
    Record::decode(u32::from(header.kind), body)
        .map_err(|error| SessionError::Log(crate::log::LogError::Undecodable { offset: at, error }))
}

/// The layout of sealed segment `object`: its header and index, read by ranges (the leading
/// bytes, the footer, then the index), never its blocks. `None` when no such object is stored.
pub(super) async fn read_layout<B: SessionBackend>(
    backend: &B,
    addr: &SessionAddr,
    object: &str,
    key: &FrameKey,
) -> Result<Option<SegmentLayout>, SessionError> {
    let Some(size) = backend.object_size(addr, object).await? else {
        return Ok(None);
    };
    let read = |range: std::ops::Range<u64>| async move {
        backend
            .get_object_range(addr, object, range)
            .await?
            .ok_or_else(|| SessionError::Fold(format!("sealed segment {object} is missing")))
    };
    let prefix = read(0..size.min(OPEN_PREFIX)).await?;
    let header_end = SegmentLayout::header_end(&prefix).map_err(SessionError::Segment)?;
    let header = if header_end <= prefix.len() as u64 {
        prefix
    } else {
        read(0..header_end.min(size)).await?
    };
    let footer = read(size.saturating_sub(FOOTER)..size).await?;
    let (offset, len) = SegmentLayout::index_span(&footer, size).map_err(SessionError::Segment)?;
    let sealed = read(offset..offset + len).await?;
    SegmentLayout::open(&header, &sealed, key, object.as_bytes())
        .map(Some)
        .map_err(SessionError::Segment)
}

/// The records of block `position` of sealed segment `object`, whose layout is `layout`: fetches
/// that block's bytes alone.
pub(super) async fn read_block<B: SessionBackend>(
    backend: &B,
    addr: &SessionAddr,
    object: &str,
    layout: &SegmentLayout,
    position: usize,
    key: &FrameKey,
) -> Result<Vec<(u64, Record)>, SessionError> {
    let (offset, len) = layout.block_span(position).map_err(SessionError::Segment)?;
    let sealed = backend
        .get_object_range(addr, object, offset..offset + len)
        .await?
        .ok_or_else(|| SessionError::Fold(format!("sealed segment {object} is missing")))?;
    layout
        .block(position, &sealed, key, object.as_bytes())
        .map_err(SessionError::Segment)
}

/// The checkpoint the session's first log starts from: empty state.
pub(super) fn first_log_checkpoint(head: &Head) -> Checkpoint {
    Checkpoint {
        covers_through: 0,
        next_seq: 1,
        conversation: Conversation::new(head.appends_in_conversation.unwrap_or(false)),
        operational: Default::default(),
        transcript_base: None,
        ledger_base: None,
        activity_ms: None,
        media: None,
    }
}

/// A session's stored records as they were when it was taken, read over a backend handle of the
/// reader's own (see the module docs).
pub struct StoredRecords<R> {
    backend: R,
    addr: SessionAddr,
    head: Head,
    key: DataKey,
    log: Id128,
    kept: Kept,
}

/// The log holding a seq, from its start: the checkpoint it started from and its records from
/// there through a last seq.
pub struct LogFrom {
    pub checkpoint: Checkpoint,
    /// In seq order: every record past the checkpoint, and before them the log's own
    /// transcript records the checkpoint's conversation doesn't hold yet (those a seal at a cut
    /// kept in the log, with their seqs).
    pub records: Vec<(u64, Record)>,
}

impl<B: SessionBackend> Session<B> {
    /// This session's stored records as of now, read over `backend`, a handle on the same
    /// storage that only reads.
    pub fn stored_records<R: SessionBackend>(&self, backend: R) -> StoredRecords<R> {
        StoredRecords {
            backend,
            addr: self.addr.clone(),
            head: self.head.clone(),
            key: self.key.clone(),
            log: self.log,
            kept: Kept::default(),
        }
    }
}

impl<R: SessionBackend> StoredRecords<R> {
    pub(super) fn place(&self) -> Place<'_, R> {
        Place {
            backend: &self.backend,
            addr: &self.addr,
            head: &self.head,
            key: &self.key,
            log: &self.log,
            kept: Some(&self.kept),
        }
    }

    /// The head's incarnation, which changes when the session's history is replaced.
    pub fn incarnation(&self) -> [u8; 16] {
        self.head.incarnation.0
    }

    /// Every record of both streams from seq `first` through seq `last`, in seq order. The first
    /// read of the open log reads it whole; later reads read only the parts of it they need.
    pub async fn records_between(
        &self,
        first: u64,
        last: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.place().records_between(first, last).await
    }

    /// The log holding `seq`, from its start through `last`: the newest log whose checkpoint
    /// covers less than `seq`, so folding its records from that checkpoint reaches `seq` without
    /// folding the session's earlier history. Records of later logs through `last` follow in seq
    /// order.
    pub async fn from_log_holding(&self, seq: u64, last: u64) -> Result<LogFrom, SessionError> {
        from_log_holding(&self.place(), seq, last).await
    }

    /// The checkpoint [`Self::from_log_holding`] starts from for `seq`, without its records, for
    /// a reader that replays them a part at a time with [`Self::replay_between`] from
    /// [`replay_start`].
    pub async fn log_start(&self, seq: u64) -> Result<Checkpoint, SessionError> {
        let place = self.place();
        let (open, _) = place.open_records(|_| None).await?;
        if open.checkpoint.covers_through < seq {
            return Ok(open.checkpoint.clone());
        }
        place.sealed_log_start(seq).await
    }

    /// The part from seq `first` through seq `last` of the records [`Self::from_log_holding`]
    /// returns for the log that started from `checkpoint`, in seq order.
    pub async fn replay_between(
        &self,
        checkpoint: &Checkpoint,
        first: u64,
        last: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        self.place()
            .wanted(Want::replay(checkpoint, first, last), None)
            .await
    }
}

/// The first seq a replay of the log that started from `checkpoint` reads: the log's records
/// start past the ledger records the checkpoint covers, and its transcript records past the last
/// transcript record sealed before it. A log whose checkpoint names no such record (no seal has
/// sealed transcript yet: only ledger-only seals came before it) holds the session's transcript
/// from its first record.
pub fn replay_start(checkpoint: &Checkpoint) -> u64 {
    checkpoint
        .transcript_base
        .unwrap_or(0)
        .min(checkpoint.covers_through)
        .saturating_add(1)
}

/// [`StoredRecords::from_log_holding`] over `place`. The open log is read once: whole, keeping
/// only its records the answer holds, unless the place already knows where they lie.
pub(super) async fn from_log_holding<B: SessionBackend>(
    place: &Place<'_, B>,
    seq: u64,
    last: u64,
) -> Result<LogFrom, SessionError> {
    // When the open log holds `seq`, the answer is its records through `last`, all of them.
    let (open, records) = place
        .open_records(|start| (start.covers_through < seq).then(|| Want::between(0, last)))
        .await?;
    if open.checkpoint.covers_through < seq {
        return Ok(LogFrom {
            checkpoint: open.checkpoint.clone(),
            records,
        });
    }
    let checkpoint = place.sealed_log_start(seq).await?;
    let want = Want::replay(&checkpoint, replay_start(&checkpoint), last);
    let records = place.wanted(want, Some(&open)).await?;
    Ok(LogFrom {
        checkpoint,
        records,
    })
}
