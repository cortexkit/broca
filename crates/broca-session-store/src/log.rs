//! The open log: one append-only file per session, holding the active window and the ledger
//! records written since the last seal, as encrypted frames in all-or-nothing batches.
//!
//! A log starts with its checkpoint batch: frames with no stream and no seq, carrying the
//! state the log's records fold onto. Every append after that is one batch, written in one call:
//! each frame names its batch's first seq and frame count, so a batch with a frame missing reads
//! as never written, and a tool result and its ledger outcome survive a crash together or not at
//! all. Every frame also carries the writer's sync claim (`synced_through`, the offset its last
//! completed barrier made durable) and a back-link to the previous record of its own stream.
//!
//! A writer's batches take consecutive seqs. A batch copied into a new log by a change (see
//! [`LogEncoder::copy_batch`]) keeps its records' seqs, which may skip the seqs of records left in
//! the old log, so the read rule is only that seqs increase. A removed frame still reads as
//! damage: frame indexes have no gaps, and every frame names its batch's frame count.
//!
//! Reading a log reports how it ends:
//! - **clean:** every batch is complete and every frame authenticates;
//! - **a torn tail:** the last batch is incomplete, which a crash in the middle of a write leaves.
//!   An empty log is one too: its checkpoint's write, cut short;
//! - **damage:** a frame that fails its header checksum or authentication, a gap in the frame
//!   indexes, or a seq or back-link that breaks the chain. Damage is never read as a torn tail.
//!
//! It also reports the durable bound: the largest sync claim among frames that authenticate, or
//! the length synced when the log was created, whichever is larger (the session head records
//! that length when it switches to the new log). Bytes below the bound were acknowledged
//! durable. Whether damage is debris of unsynced writes or corruption of acknowledged bytes is
//! recovery's decision, made from that bound; this module only reports.

use std::io::{self, Write};

use crate::codec::DecodeError;
use crate::frame::{
    self, FrameError, FrameHeader, FrameKey, FrameRead, FrameStream, FLAG_CLAIM_ONLY,
};
use crate::record::{Record, Stream};

#[cfg(test)]
mod tests;

/// Where a log is written: its bytes, and a barrier that makes them durable.
pub trait LogFile: Write {
    /// Returns once every byte written so far is durable. On macOS, `File::sync_all` issues
    /// `F_FULLFSYNC`, which survives power loss; a plain `fsync` there does not.
    fn barrier(&mut self) -> io::Result<()>;
}

impl LogFile for std::fs::File {
    fn barrier(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

/// A log held in memory, for tests and measurements; its barrier is a no-op.
impl LogFile for Vec<u8> {
    fn barrier(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum LogError {
    Io(io::Error),
    Frame(FrameError),
    /// A batch holds more records than a frame header can count.
    BatchTooLarge(usize),
    /// Copied records whose seqs don't increase, don't follow the log's last record, or aren't
    /// below the seq the next new record takes.
    CopiedSeqs,
    /// A frame authenticated, so its bytes are what was written, but its record doesn't decode.
    /// The log is refused, wherever the frame lies.
    Undecodable {
        offset: u64,
        error: DecodeError,
    },
    /// A record batch was encoded before the log's checkpoint, or a checkpoint after it.
    CheckpointNotFirst,
    /// The checkpoint authenticated, so its bytes are what was written, but it doesn't decode.
    Checkpoint(String),
}

impl From<io::Error> for LogError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<FrameError> for LogError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

/// The seq of the last record of each stream before a log's first record: the back-links the
/// log's first record of each stream names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChainBase {
    pub transcript: Option<u64>,
    pub ledger: Option<u64>,
}

impl ChainBase {
    /// The last record of `stream` this base names.
    pub fn get(&self, stream: Stream) -> Option<u64> {
        match stream {
            Stream::Transcript => self.transcript,
            Stream::Ledger => self.ledger,
        }
    }

    fn slot(&mut self, stream: Stream) -> &mut Option<u64> {
        match stream {
            Stream::Transcript => &mut self.transcript,
            Stream::Ledger => &mut self.ledger,
        }
    }
}

fn frame_stream(stream: Stream) -> FrameStream {
    match stream {
        Stream::Transcript => FrameStream::Transcript,
        Stream::Ledger => FrameStream::Ledger,
    }
}

/// One encoded append: the bytes to write in one call, and what the log looks like once they
/// land.
#[derive(Debug, Clone)]
pub struct Encoded {
    pub bytes: Vec<u8>,
    frames: u32,
    /// The seq the batch's first record takes; `None` for frames that carry no record.
    pub first_seq: Option<u64>,
    next_seq: u64,
    last: ChainBase,
    last_seq: Option<u64>,
}

impl Encoded {
    /// Records this append as written by `encoder`, and returns its bytes, for a writer that
    /// writes several appends in one call.
    pub fn into_written(self, encoder: &mut LogEncoder) -> Vec<u8> {
        encoder.written(&self);
        self.bytes
    }

    /// Where each frame of this append lies once its bytes are written at offset `at`, in order:
    /// for a batch, each record's frame. Read from each frame's own length field, so an append
    /// carries nothing more than its bytes for it.
    pub fn places(&self, at: u64) -> impl Iterator<Item = FramePlace> + '_ {
        let mut offset = 0usize;
        std::iter::from_fn(move || {
            let length = self.bytes.get(offset..offset + 4)?;
            let len = u32::from_le_bytes(length.try_into().expect("four bytes")).checked_add(4)?;
            let place = FramePlace {
                at: at + offset as u64,
                len,
            };
            offset += len as usize;
            Some(place)
        })
    }
}

/// Where one record's frame lies in its log: its first byte and its length, which is enough to
/// read that one record back without reading the rest of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FramePlace {
    pub at: u64,
    pub len: u32,
}

/// A frame's length as a [`FramePlace`] holds it. A frame's length field is a `u32` counting the
/// bytes after it, and its body is bounded by [`frame::MAX_BODY`], so every frame fits.
fn frame_len(len: usize) -> u32 {
    u32::try_from(len).expect("a frame's length fits in 32 bits")
}

/// The length of the first frame in `bytes`, read from its own length field, when `bytes` holds
/// at least that field. A log's first frame is its checkpoint.
pub fn first_frame_len(bytes: &[u8]) -> Option<u32> {
    let length = bytes.get(..4)?;
    u32::from_le_bytes(length.try_into().expect("four bytes")).checked_add(4)
}

/// Whether a read of a log, from offset `start`, that returned `bytes` out of a log `log_len`
/// bytes long read only the log's checkpoint frame: it started at 0, it holds exactly the length
/// the first frame records, and the log goes on past it. A read that reaches the log's end, or
/// holds more than the first frame, is a read of the log, so a whole-log read never counts as
/// one. For backends that count their reads in tests and measurements.
#[doc(hidden)]
pub fn reads_checkpoint_frame_only(start: u64, bytes: &[u8], log_len: u64) -> bool {
    start == 0
        && first_frame_len(bytes).is_some_and(|len| len as usize == bytes.len())
        && (bytes.len() as u64) < log_len
}

/// Encodes the frames of one log. It holds the log's position (frame index, length, sync claim,
/// next seq and each stream's last record) but does no I/O, so any backend can write what it
/// encodes. An encoded append changes nothing until [`written`](Self::written) reports that it
/// landed; a writer whose write fails stops and reloads, so it never encodes past a write it
/// isn't sure of.
#[derive(Clone)]
pub struct LogEncoder {
    key: FrameKey,
    bind: Vec<u8>,
    frame_index: u32,
    offset: u64,
    synced_through: u64,
    next_seq: u64,
    last: ChainBase,
    /// The seq of the last record written to this log.
    last_seq: Option<u64>,
}

/// The encoder's key holds its cipher inline, so only its binding is on the heap.
impl crate::held::HeldBytes for LogEncoder {
    fn heap_bytes(&self) -> usize {
        self.bind.capacity()
    }
}

impl LogEncoder {
    /// An encoder for a new, empty log. `bind` names the log (the session id and the log's
    /// name); every reader must pass the same bytes. `next_seq` is the first seq a record in
    /// this log may take, and `base` names the last record of each stream before it.
    pub fn new(key: FrameKey, bind: Vec<u8>, next_seq: u64, base: ChainBase) -> Self {
        Self {
            key,
            bind,
            frame_index: 0,
            offset: 0,
            synced_through: 0,
            next_seq,
            last: base,
            last_seq: None,
        }
    }

    /// An encoder that continues a log read back with [`read`], once its torn tail (if any) is
    /// removed. What the earlier writer wrote past the durable bound may not be durable yet, so
    /// the sync claim restarts from the bound, and the next barrier claims the rest.
    pub fn resume(key: FrameKey, bind: Vec<u8>, log: &ReadLog, next_seq: u64) -> Self {
        Self {
            key,
            bind,
            frame_index: log.frames,
            offset: log.complete_through,
            synced_through: log.durable_bound.min(log.complete_through),
            next_seq,
            last: log.last,
            last_seq: log.last_seq,
        }
    }

    fn header_without_record(&self, flags: u8) -> FrameHeader {
        FrameHeader {
            flags,
            stream: FrameStream::None,
            kind: 0,
            kind_version: 0,
            key_id: self.key.id(),
            seq: 0,
            batch_first: 0,
            batch_count: 1,
            frame_index: self.frame_index,
            synced_through: self.synced_through,
            prev_in_stream: None,
        }
    }

    fn without_record(&self, flags: u8, body: &[u8]) -> Result<Encoded, LogError> {
        let header = self.header_without_record(flags);
        Ok(Encoded {
            bytes: frame::seal(&header, body, &self.key, &self.bind)?,
            frames: 1,
            first_seq: None,
            next_seq: self.next_seq,
            last: self.last,
            last_seq: self.last_seq,
        })
    }

    /// The log's checkpoint batch, which must be its first frame.
    pub fn checkpoint(&self, body: &[u8]) -> Result<Encoded, LogError> {
        if self.frame_index != 0 {
            return Err(LogError::CheckpointNotFirst);
        }
        self.without_record(0, body)
    }

    /// `records` as one batch, to be written in one call. The batch is not durable until a
    /// barrier after it returns.
    pub fn batch(&self, records: &[Record]) -> Result<Encoded, LogError> {
        if self.frame_index == 0 {
            return Err(LogError::CheckpointNotFirst);
        }
        let count = u16::try_from(records.len())
            .ok()
            .filter(|count| *count > 0)
            .ok_or(LogError::BatchTooLarge(records.len()))?;
        let first = self.next_seq;
        let mut last = self.last;
        let mut bytes = Vec::new();
        for (position, record) in records.iter().enumerate() {
            let seq = first + position as u64;
            let stream = record.stream();
            let header = FrameHeader {
                flags: 0,
                stream: frame_stream(stream),
                kind: u8::try_from(record.kind())
                    .map_err(|_| FrameError::BadHeader("record kind above 255"))?,
                kind_version: 1,
                key_id: self.key.id(),
                seq,
                batch_first: first,
                batch_count: count,
                frame_index: self.frame_index + position as u32,
                synced_through: self.synced_through,
                prev_in_stream: last.slot(stream).replace(seq),
            };
            bytes.extend(frame::seal(
                &header,
                &record.encode_body(),
                &self.key,
                &self.bind,
            )?);
        }
        Ok(Encoded {
            bytes,
            frames: u32::from(count),
            first_seq: Some(first),
            next_seq: first + u64::from(count),
            last,
            last_seq: Some(first + u64::from(count) - 1),
        })
    }

    /// Records copied from another log as one batch, keeping their seqs, which must increase
    /// and follow every seq this log holds. A change copies each batch of the records it keeps
    /// this way, so the batch rule holds in the new log too.
    pub fn copy_batch(&self, records: &[(u64, &Record)]) -> Result<Encoded, LogError> {
        if self.frame_index == 0 {
            return Err(LogError::CheckpointNotFirst);
        }
        let count = u16::try_from(records.len())
            .ok()
            .filter(|count| *count > 0)
            .ok_or(LogError::BatchTooLarge(records.len()))?;
        let first = records[0].0;
        let final_seq = records.last().expect("non-empty").0;
        let ordered = records.windows(2).all(|pair| pair[1].0 > pair[0].0)
            && self.last_seq.is_none_or(|last| first > last)
            && final_seq < self.next_seq;
        if !ordered {
            return Err(LogError::CopiedSeqs);
        }
        let mut last = self.last;
        let mut bytes = Vec::new();
        for (position, (seq, record)) in records.iter().enumerate() {
            let stream = record.stream();
            let header = FrameHeader {
                flags: 0,
                stream: frame_stream(stream),
                kind: u8::try_from(record.kind())
                    .map_err(|_| FrameError::BadHeader("record kind above 255"))?,
                kind_version: 1,
                key_id: self.key.id(),
                seq: *seq,
                batch_first: first,
                batch_count: count,
                frame_index: self.frame_index + position as u32,
                synced_through: self.synced_through,
                prev_in_stream: last.slot(stream).replace(*seq),
            };
            bytes.extend(frame::seal(
                &header,
                &record.encode_body(),
                &self.key,
                &self.bind,
            )?);
        }
        Ok(Encoded {
            bytes,
            frames: u32::from(count),
            first_seq: Some(first),
            next_seq: self.next_seq,
            last,
            last_seq: Some(final_seq),
        })
    }

    /// A claim-only frame, which carries no record, only the claim of the last barrier. A
    /// writer appends one when it goes idle or closes, so the log's last barrier counts toward
    /// the durable bound even though no later batch claims it. It needs no barrier of its own:
    /// a claim that survives and authenticates is true.
    pub fn claim(&self) -> Result<Encoded, LogError> {
        self.without_record(FLAG_CLAIM_ONLY, &[])
    }

    /// Records that `encoded` was written at the end of the log.
    pub fn written(&mut self, encoded: &Encoded) {
        self.offset += encoded.bytes.len() as u64;
        self.frame_index += encoded.frames;
        self.next_seq = encoded.next_seq;
        self.last = encoded.last;
        self.last_seq = encoded.last_seq;
    }

    /// Records that a barrier made everything written so far durable. Later frames claim it.
    pub fn synced(&mut self) {
        self.synced_through = self.offset;
    }

    /// Whether bytes were written since the last barrier made the log durable.
    pub fn unsynced(&self) -> bool {
        self.offset > self.synced_through
    }

    /// The log's length so far.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The byte boundary made durable by the writer's most recent successful barrier.
    pub fn synced_through(&self) -> u64 {
        self.synced_through
    }

    /// The seq the next record will take.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Each stream's last record so far.
    pub fn last(&self) -> ChainBase {
        self.last
    }
}

/// Appends batches to one log held in a [`LogFile`]: an encoder plus the file it writes. It is
/// the log's only writer.
pub struct LogWriter<F: LogFile> {
    file: F,
    encoder: LogEncoder,
}

impl<F: LogFile> LogWriter<F> {
    /// Writes a new log's checkpoint batch and syncs it. The returned writer's
    /// [`offset`](Self::offset) is the length synced when the log was created, which the
    /// session head records with the log's name.
    pub fn create(
        file: F,
        key: FrameKey,
        bind: Vec<u8>,
        checkpoint: &[u8],
        next_seq: u64,
        base: ChainBase,
    ) -> Result<Self, LogError> {
        let mut writer = Self {
            file,
            encoder: LogEncoder::new(key, bind, next_seq, base),
        };
        let encoded = writer.encoder.checkpoint(checkpoint)?;
        writer.write(&encoded)?;
        writer.barrier()?;
        Ok(writer)
    }

    fn write(&mut self, encoded: &Encoded) -> Result<(), LogError> {
        self.file.write_all(&encoded.bytes)?;
        self.encoder.written(encoded);
        Ok(())
    }

    /// Appends `records` as one batch, in one write, and returns the first seq they took. The
    /// batch is not durable until a later [`barrier`](Self::barrier) returns.
    pub fn append(&mut self, records: &[Record]) -> Result<u64, LogError> {
        let encoded = self.encoder.batch(records)?;
        self.write(&encoded)?;
        Ok(encoded.first_seq.expect("a batch holds records"))
    }

    /// Makes everything written so far durable. Later frames claim it as synced.
    pub fn barrier(&mut self) -> Result<(), LogError> {
        self.file.flush()?;
        self.file.barrier()?;
        self.encoder.synced();
        Ok(())
    }

    /// Appends a claim-only frame; see [`LogEncoder::claim`].
    pub fn claim(&mut self) -> Result<(), LogError> {
        let encoded = self.encoder.claim()?;
        self.write(&encoded)
    }

    /// The log's length so far.
    pub fn offset(&self) -> u64 {
        self.encoder.offset()
    }

    /// The seq the next record will take.
    pub fn next_seq(&self) -> u64 {
        self.encoder.next_seq()
    }

    pub fn into_file(self) -> F {
        self.file
    }
}

/// Why a log's reading stopped short of its end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Damage {
    /// A frame failed its header checksum or authentication, or its header is out of range.
    Frame(FrameError),
    /// The log doesn't start with a checkpoint batch.
    NoCheckpoint,
    /// A frame's index isn't the next one, so a frame was removed, moved or spliced in.
    IndexGap { expected: u32, found: u32 },
    /// A record's seq doesn't follow the previous record's.
    SeqOrder { previous: u64, found: u64 },
    /// A frame doesn't belong to the batch it should continue.
    BatchMismatch,
    /// A record's back-link doesn't name the previous record of its stream.
    BackLink {
        expected: Option<u64>,
        found: Option<u64>,
    },
}

/// How a log ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEnd {
    Clean,
    /// The batch starting at `at` is incomplete: a write that never finished.
    TornTail {
        at: u64,
    },
    /// Reading stopped at the frame starting at `at`.
    Damaged {
        at: u64,
        damage: Damage,
    },
}

/// A log as read: its checkpoint, its records with their seqs, and how it ends.
#[derive(Debug)]
pub struct ReadLog {
    pub checkpoint: Vec<u8>,
    /// Records of complete batches only: an incomplete batch never committed. Empty when the
    /// log was read batch by batch with a [`LogReader`], which hands each batch on instead of
    /// collecting it.
    pub records: Vec<(u64, Record)>,
    pub end: LogEnd,
    /// The largest sync claim among frames that authenticated, or the creation length if
    /// larger. Bytes below it were acknowledged durable.
    pub durable_bound: u64,
    /// Where the complete batches end: the length a writer resumes appending at.
    pub complete_through: u64,
    /// Frames in the complete batches, the checkpoint and claim-only frames included: the
    /// frame index a writer resumes at.
    pub frames: u32,
    /// Each stream's last record in the complete batches, or the checkpoint's base when a
    /// stream has none.
    pub last: ChainBase,
    /// The seq of the last record in the complete batches, if any.
    pub last_seq: Option<u64>,
    /// Each complete batch, as a range of indexes into `records`.
    pub batches: Vec<std::ops::Range<usize>>,
    /// Where each complete batch starts in the log, in the order of `batches`: a length the log
    /// can be cut back to and still end on a complete batch.
    pub batch_starts: Vec<u64>,
}

/// One complete batch, as a [`LogReader`] reads it: where it starts in the log, its records with
/// their seqs, and where each record's frame lies.
pub struct ReadBatch {
    pub at: u64,
    pub records: Vec<(u64, Record)>,
    pub places: Vec<FramePlace>,
}

/// A batch read so far, kept apart until its last frame arrives.
struct OpenBatch {
    at: u64,
    first: u64,
    count: u16,
    frames: u16,
    records: Vec<(u64, Record)>,
    places: Vec<FramePlace>,
    last: ChainBase,
}

/// Reads a log from its bytes. `created_len` is the length synced when the log was created,
/// which counts toward the durable bound. `base` reads the chain base from the checkpoint's
/// body: each stream's first record must link back to exactly that base, and every later record
/// to the record before it in its stream.
pub fn read(
    bytes: &[u8],
    key: &FrameKey,
    bind: &[u8],
    created_len: u64,
    base: impl FnOnce(&[u8]) -> Result<ChainBase, String>,
) -> Result<ReadLog, LogError> {
    let mut reader = LogReader::new(bytes, key, bind, created_len, base);
    let mut records = Vec::new();
    let mut batches = Vec::new();
    let mut batch_starts = Vec::new();
    while let Some(batch) = reader.next_batch()? {
        let start = records.len();
        records.extend(batch.records);
        batches.push(start..records.len());
        batch_starts.push(batch.at);
    }
    let mut log = reader.finish();
    log.records = records;
    log.batches = batches;
    log.batch_starts = batch_starts;
    Ok(log)
}

/// Reads a log from its bytes batch by batch, with the checks [`read`] makes, so a caller can
/// fold each batch and drop it before the next is decoded, never holding the whole decoded log.
/// A batch is handed on only once its last frame is read, so the batches handed on before the
/// reading stops are exactly the records [`read`] returns, and [`LogReader::finish`] gives the
/// rest of what [`read`] returns: how the log ends, its bounds and its checkpoint.
pub struct LogReader<'a, F> {
    bytes: &'a [u8],
    key: &'a FrameKey,
    bind: &'a [u8],
    base: Option<F>,
    log: ReadLog,
    offset: usize,
    index: u32,
    last_seq: Option<u64>,
    chain: ChainBase,
    open: Option<OpenBatch>,
    /// The reading stopped: at the log's end, a torn tail or damage.
    stopped: bool,
}

impl<'a, F: FnOnce(&[u8]) -> Result<ChainBase, String>> LogReader<'a, F> {
    /// A reader of the log `bytes`, as [`read`] reads it.
    pub fn new(
        bytes: &'a [u8],
        key: &'a FrameKey,
        bind: &'a [u8],
        created_len: u64,
        base: F,
    ) -> Self {
        let mut reader = Self {
            bytes,
            key,
            bind,
            base: Some(base),
            log: ReadLog {
                checkpoint: Vec::new(),
                records: Vec::new(),
                end: LogEnd::Clean,
                durable_bound: created_len,
                complete_through: 0,
                frames: 0,
                last: ChainBase::default(),
                last_seq: None,
                batches: Vec::new(),
                batch_starts: Vec::new(),
            },
            offset: 0,
            index: 0,
            last_seq: None,
            chain: ChainBase::default(),
            open: None,
            stopped: false,
        };
        if bytes.is_empty() {
            // A log is created by writing and syncing its checkpoint, so an empty one is that
            // write cut short: a torn tail at 0, which the durable bound judges, never a clean log.
            reader.log.end = LogEnd::TornTail { at: 0 };
            reader.stopped = true;
        }
        reader
    }

    /// The checkpoint's body, once read; empty before.
    pub fn checkpoint(&self) -> &[u8] {
        &self.log.checkpoint
    }

    /// The log's next complete batch, or `None` once the reading stopped.
    pub fn next_batch(&mut self) -> Result<Option<ReadBatch>, LogError> {
        let damaged = |log: &mut ReadLog, at: usize, damage: Damage| {
            log.end = LogEnd::Damaged {
                at: at as u64,
                damage,
            };
        };
        while !self.stopped && self.offset < self.bytes.len() {
            let key = self.key;
            let read = frame::open(
                &self.bytes[self.offset..],
                |id| (id == key.id()).then_some(key),
                self.bind,
            );
            let (header, body, len) = match read {
                Ok(FrameRead::Frame { header, body, len }) => (header, body, len),
                Ok(FrameRead::Incomplete) => {
                    let at = self
                        .open
                        .as_ref()
                        .map_or(self.offset as u64, |batch| batch.at);
                    self.log.end = LogEnd::TornTail { at };
                    self.stopped = true;
                    return Ok(None);
                }
                Err(error) => {
                    damaged(&mut self.log, self.offset, Damage::Frame(error));
                    self.stopped = true;
                    return Ok(None);
                }
            };
            if header.frame_index != self.index {
                damaged(
                    &mut self.log,
                    self.offset,
                    Damage::IndexGap {
                        expected: self.index,
                        found: header.frame_index,
                    },
                );
                self.stopped = true;
                return Ok(None);
            }
            self.log.durable_bound = self.log.durable_bound.max(header.synced_through);
            let at = self.offset as u64;
            self.offset += len;
            self.index += 1;

            if self.index == 1 {
                // The checkpoint batch: one frame with no stream and no seq.
                if header.stream != FrameStream::None || header.flags & FLAG_CLAIM_ONLY != 0 {
                    damaged(&mut self.log, at as usize, Damage::NoCheckpoint);
                    self.stopped = true;
                    return Ok(None);
                }
                let base = self.base.take().expect("the checkpoint is the first frame");
                self.chain = base(&body).map_err(LogError::Checkpoint)?;
                self.log.checkpoint = body;
                self.log.complete_through = self.offset as u64;
                self.log.frames = self.index;
                self.log.last = self.chain;
                continue;
            }
            if header.stream == FrameStream::None {
                if header.flags & FLAG_CLAIM_ONLY == 0 || self.open.is_some() {
                    damaged(&mut self.log, at as usize, Damage::BatchMismatch);
                    self.stopped = true;
                    return Ok(None);
                }
                self.log.complete_through = self.offset as u64;
                self.log.frames = self.index;
                continue;
            }

            let stream = match header.stream {
                FrameStream::Transcript => Stream::Transcript,
                _ => Stream::Ledger,
            };
            if let Some(previous) = self.last_seq.filter(|previous| header.seq <= *previous) {
                damaged(
                    &mut self.log,
                    at as usize,
                    Damage::SeqOrder {
                        previous,
                        found: header.seq,
                    },
                );
                self.stopped = true;
                return Ok(None);
            }
            let batch = match self.open.as_mut() {
                Some(batch) => {
                    // Seqs increase across the log, checked above; inside a copied batch they
                    // may skip.
                    if header.batch_first != batch.first || header.batch_count != batch.count {
                        damaged(&mut self.log, at as usize, Damage::BatchMismatch);
                        self.stopped = true;
                        return Ok(None);
                    }
                    batch
                }
                None => {
                    if header.seq != header.batch_first || header.batch_count == 0 {
                        damaged(&mut self.log, at as usize, Damage::BatchMismatch);
                        self.stopped = true;
                        return Ok(None);
                    }
                    self.open.insert(OpenBatch {
                        at,
                        first: header.seq,
                        count: header.batch_count,
                        frames: 0,
                        records: Vec::new(),
                        places: Vec::new(),
                        last: self.chain,
                    })
                }
            };
            let expected_link = *batch.last.slot(stream);
            if header.prev_in_stream != expected_link {
                damaged(
                    &mut self.log,
                    at as usize,
                    Damage::BackLink {
                        expected: expected_link,
                        found: header.prev_in_stream,
                    },
                );
                self.stopped = true;
                return Ok(None);
            }
            *batch.last.slot(stream) = Some(header.seq);
            let record = Record::decode(u32::from(header.kind), &body)
                .map_err(|error| LogError::Undecodable { offset: at, error })?;
            batch.records.push((header.seq, record));
            batch.places.push(FramePlace {
                at,
                len: frame_len(len),
            });
            batch.frames += 1;
            self.last_seq = Some(header.seq);
            if batch.frames == batch.count {
                let batch = self.open.take().expect("the batch being read");
                self.chain = batch.last;
                self.log.complete_through = self.offset as u64;
                self.log.frames = self.index;
                self.log.last = self.chain;
                self.log.last_seq = Some(header.seq);
                return Ok(Some(ReadBatch {
                    at: batch.at,
                    records: batch.records,
                    places: batch.places,
                }));
            }
        }
        if !self.stopped {
            self.stopped = true;
            if let Some(batch) = &self.open {
                self.log.end = LogEnd::TornTail { at: batch.at };
            }
        }
        Ok(None)
    }

    /// The log as read once [`Self::next_batch`] returned `None`: how it ends, its bounds and
    /// its checkpoint, without the records, which were handed on batch by batch.
    pub fn finish(self) -> ReadLog {
        self.log
    }
}
