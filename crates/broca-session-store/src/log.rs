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
//! Reading a log reports how it ends:
//! - **clean:** every batch is complete and every frame authenticates;
//! - **a torn tail:** the last batch is incomplete, which a crash in the middle of a write leaves;
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
    /// A frame authenticated, so its bytes are what was written, but its record doesn't decode.
    /// The log is refused, wherever the frame lies.
    Undecodable {
        offset: u64,
        error: DecodeError,
    },
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

/// Appends batches to one log. It is the log's only writer.
pub struct LogWriter<F: LogFile> {
    file: F,
    key: FrameKey,
    bind: Vec<u8>,
    frame_index: u32,
    offset: u64,
    synced_through: u64,
    next_seq: u64,
    last: ChainBase,
}

impl<F: LogFile> LogWriter<F> {
    /// Writes a new log's checkpoint batch and syncs it. `bind` names the log (the session id
    /// and the log's name); every reader must pass the same bytes. `next_seq` is the first seq
    /// a record in this log may take, and `base` names the last record of each stream before
    /// it. The returned writer's [`offset`](Self::offset) is the length synced when the log was
    /// created, which the session head records with the log's name.
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
            key,
            bind,
            frame_index: 0,
            offset: 0,
            synced_through: 0,
            next_seq,
            last: base,
        };
        let header = writer.header_without_record(0);
        let bytes = frame::seal(&header, checkpoint, &writer.key, &writer.bind)?;
        writer.write(&bytes, 1)?;
        writer.barrier()?;
        Ok(writer)
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

    fn write(&mut self, bytes: &[u8], frames: u32) -> Result<(), LogError> {
        self.file.write_all(bytes)?;
        self.offset += bytes.len() as u64;
        self.frame_index += frames;
        Ok(())
    }

    /// Appends `records` as one batch, in one write, and returns the first seq they took. The
    /// batch is not durable until a later [`barrier`](Self::barrier) returns.
    pub fn append(&mut self, records: &[Record]) -> Result<u64, LogError> {
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
        self.write(&bytes, u32::from(count))?;
        self.next_seq = first + u64::from(count);
        self.last = last;
        Ok(first)
    }

    /// Makes everything written so far durable. Later frames claim it as synced.
    pub fn barrier(&mut self) -> Result<(), LogError> {
        self.file.flush()?;
        self.file.barrier()?;
        self.synced_through = self.offset;
        Ok(())
    }

    /// Appends a claim-only frame, which carries no record, only the claim of the last barrier.
    /// A writer appends one when it goes idle or closes, so the log's last barrier counts toward
    /// the durable bound even though no later batch claims it. It needs no barrier of its own:
    /// a claim that survives and authenticates is true.
    pub fn claim(&mut self) -> Result<(), LogError> {
        let header = self.header_without_record(FLAG_CLAIM_ONLY);
        let bytes = frame::seal(&header, &[], &self.key, &self.bind)?;
        self.write(&bytes, 1)
    }

    /// The log's length so far.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The seq the next record will take.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
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
    /// Records of complete batches only: an incomplete batch never committed.
    pub records: Vec<(u64, Record)>,
    pub end: LogEnd,
    /// The largest sync claim among frames that authenticated, or the creation length if
    /// larger. Bytes below it were acknowledged durable.
    pub durable_bound: u64,
    /// Where the complete batches end: the length a writer resumes appending at.
    pub complete_through: u64,
}

/// A batch read so far, kept apart until its last frame arrives.
struct OpenBatch {
    at: u64,
    first: u64,
    count: u16,
    frames: u16,
    records: Vec<(u64, Record)>,
    last: ChainBase,
}

/// Reads a log from its bytes. `created_len` is the length synced when the log was created,
/// which counts toward the durable bound, and `base` the chain base its checkpoint names.
pub fn read(
    bytes: &[u8],
    key: &FrameKey,
    bind: &[u8],
    created_len: u64,
    base: ChainBase,
) -> Result<ReadLog, LogError> {
    let mut log = ReadLog {
        checkpoint: Vec::new(),
        records: Vec::new(),
        end: LogEnd::Clean,
        durable_bound: created_len,
        complete_through: 0,
    };
    let mut offset = 0usize;
    let mut index = 0u32;
    let mut last_seq: Option<u64> = None;
    let mut chain = base;
    let mut open: Option<OpenBatch> = None;
    let damaged = |log: &mut ReadLog, at: usize, damage: Damage| {
        log.end = LogEnd::Damaged {
            at: at as u64,
            damage,
        };
    };
    while offset < bytes.len() {
        let read = frame::open(&bytes[offset..], |id| (id == key.id()).then_some(key), bind);
        let (header, body, len) = match read {
            Ok(FrameRead::Frame { header, body, len }) => (header, body, len),
            Ok(FrameRead::Incomplete) => {
                let at = open.as_ref().map_or(offset as u64, |batch| batch.at);
                log.end = LogEnd::TornTail { at };
                return Ok(log);
            }
            Err(error) => {
                damaged(&mut log, offset, Damage::Frame(error));
                return Ok(log);
            }
        };
        if header.frame_index != index {
            damaged(
                &mut log,
                offset,
                Damage::IndexGap {
                    expected: index,
                    found: header.frame_index,
                },
            );
            return Ok(log);
        }
        log.durable_bound = log.durable_bound.max(header.synced_through);
        let at = offset as u64;
        offset += len;
        index += 1;

        if index == 1 {
            // The checkpoint batch: one frame with no stream and no seq.
            if header.stream != FrameStream::None || header.flags & FLAG_CLAIM_ONLY != 0 {
                damaged(&mut log, at as usize, Damage::NoCheckpoint);
                return Ok(log);
            }
            log.checkpoint = body;
            log.complete_through = offset as u64;
            continue;
        }
        if header.stream == FrameStream::None {
            if header.flags & FLAG_CLAIM_ONLY == 0 || open.is_some() {
                damaged(&mut log, at as usize, Damage::BatchMismatch);
                return Ok(log);
            }
            log.complete_through = offset as u64;
            continue;
        }

        let stream = match header.stream {
            FrameStream::Transcript => Stream::Transcript,
            _ => Stream::Ledger,
        };
        if let Some(previous) = last_seq.filter(|previous| header.seq <= *previous) {
            damaged(
                &mut log,
                at as usize,
                Damage::SeqOrder {
                    previous,
                    found: header.seq,
                },
            );
            return Ok(log);
        }
        let batch = match open.as_mut() {
            Some(batch) => {
                let expected = batch.first + u64::from(batch.frames);
                if header.batch_first != batch.first
                    || header.batch_count != batch.count
                    || header.seq != expected
                {
                    damaged(&mut log, at as usize, Damage::BatchMismatch);
                    return Ok(log);
                }
                batch
            }
            None => {
                if header.seq != header.batch_first || header.batch_count == 0 {
                    damaged(&mut log, at as usize, Damage::BatchMismatch);
                    return Ok(log);
                }
                open.insert(OpenBatch {
                    at,
                    first: header.seq,
                    count: header.batch_count,
                    frames: 0,
                    records: Vec::new(),
                    last: chain,
                })
            }
        };
        let expected_link = *batch.last.slot(stream);
        if expected_link.is_some() && header.prev_in_stream != expected_link {
            damaged(
                &mut log,
                at as usize,
                Damage::BackLink {
                    expected: expected_link,
                    found: header.prev_in_stream,
                },
            );
            return Ok(log);
        }
        *batch.last.slot(stream) = Some(header.seq);
        let record = Record::decode(u32::from(header.kind), &body)
            .map_err(|error| LogError::Undecodable { offset: at, error })?;
        batch.records.push((header.seq, record));
        batch.frames += 1;
        last_seq = Some(header.seq);
        if batch.frames == batch.count {
            let batch = open.take().expect("the batch being read");
            chain = batch.last;
            log.records.extend(batch.records);
            log.complete_through = offset as u64;
        }
    }
    if let Some(batch) = open {
        log.end = LogEnd::TornTail { at: batch.at };
    }
    Ok(log)
}
