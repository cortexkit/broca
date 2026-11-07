//! Sealed segments: immutable objects holding records that left the open log.
//!
//! ```text
//! magic       "BSEG"
//! format      u8
//! header_len  u32 LE
//! header      the segment's header, readable, and authenticated by every unit below
//! blocks      one sealed unit each: records in seq order, compressed with zstd, then encrypted
//! index       one sealed unit: one entry per block, plus the segment's leading state (a
//!             transcript segment) or trailing state (a ledger segment)
//! footer      index offset (u64 LE), index length (u32 LE), "BSEG"
//! ```
//!
//! The block is the unit of reading, decompression and encryption, so reading one record costs
//! one block (16 KiB target, measured best for read cost against size). Records are never split.
//! A *group* of records, such as an assistant message with its batch's tool results, always
//! lands in one block: a group bigger than the target becomes a block of its own, so its
//! neighbours never pay to read it.
//!
//! Every sealed unit authenticates the session id, the object's name, the unit's position (its
//! block number, or the index's), the key id and the whole header. So a block can't be moved to
//! another position or object, and a segment can't be served under another name.

use std::ops::Range;

use crate::codec::{ByteBuf, DecodeError, Id128, Message as _};
use crate::frame::FrameKey;
use crate::record::{Record, Stream};

pub const SEGMENT_MAGIC: &[u8; 4] = b"BSEG";
pub const SEGMENT_FORMAT: u8 = 1;
/// A block is closed when the next group would take it past this many encoded bytes.
pub const BLOCK_TARGET: usize = 16 * 1024;
/// zstd level 3: the level the block-size measurement used.
const ZSTD_LEVEL: i32 = 3;
const UNIT_CONTEXT: &[u8] = b"broca-segment-v1";
const INDEX_POSITION: u32 = u32::MAX;
const FOOTER_LEN: usize = 8 + 4 + 4;
/// The most a block may decompress to.
pub const MAX_BLOCK: usize = 64 << 20;

crate::message! {
    /// One record inside a block.
    #[derive(Debug, Clone, PartialEq)]
    pub struct StoredRecord {
        1 req seq: u64,
        2 req kind: u32,
        3 req body: ByteBuf,
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq)]
    pub struct BlockBody {
        1 rep records: StoredRecord,
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct BlockEntry {
        1 req first_seq: u64,
        2 req offset: u64,
        3 req len: u64,
        4 req records: u64,
        /// In a transcript segment: the ordinal the first message placed in this block gets
        /// (see `fold::conversation`). A block always starts a group, where no tool turn is
        /// open, so its messages can be rebuilt from it alone.
        5 opt first_ordinal: u64,
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SegmentHeader {
        1 req format: u32,
        2 req session_id: Id128,
        /// 0 for a transcript segment, 1 for a ledger segment.
        3 req stream: u32,
        4 req first_seq: u64,
        5 req last_seq: u64,
        6 req created_at_ms: u64,
        /// The changes whose append-time objects (media, oversized bodies) the records name, so
        /// the segment's lifetime keeps those objects alive.
        7 rep prefixes: Id128,
    }
}

crate::message! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SegmentIndex {
        1 rep blocks: BlockEntry,
        /// A transcript segment's leading state: the conversation state at its first record.
        2 opt leading: ByteBuf,
        /// A ledger segment's trailing state: the operational state at its last record.
        3 opt trailer: ByteBuf,
        /// For the ledger segment a change sealed when it created a new open log: that log's
        /// checkpoint, exactly as written. A damaged checkpoint is rebuilt from it.
        4 opt log_start: ByteBuf,
    }
}

#[derive(Debug)]
pub enum SegmentError {
    Malformed(&'static str),
    Decode(DecodeError),
    /// A unit failed to authenticate: the bytes were changed, moved, or read under another name
    /// or key.
    Authentication,
    Compression(String),
    /// Records of the other stream, or not in increasing seq order.
    BadRecords(&'static str),
}

fn stream_code(stream: Stream) -> u32 {
    match stream {
        Stream::Transcript => 0,
        Stream::Ledger => 1,
    }
}

pub(crate) fn unit_aad(header: &[u8], key: &FrameKey, name: &[u8], position: u32) -> Vec<u8> {
    let mut aad = Vec::with_capacity(UNIT_CONTEXT.len() + name.len() + header.len() + 16);
    aad.extend_from_slice(UNIT_CONTEXT);
    aad.extend_from_slice(&key.id().to_le_bytes());
    aad.extend_from_slice(&(name.len() as u32).to_le_bytes());
    aad.extend_from_slice(name);
    aad.extend_from_slice(&position.to_le_bytes());
    aad.extend_from_slice(header);
    aad
}

/// What a segment holds besides its records.
pub struct SegmentMeta {
    pub session_id: Id128,
    pub created_at_ms: u64,
    pub prefixes: Vec<Id128>,
    pub leading: Option<Vec<u8>>,
    pub trailer: Option<Vec<u8>>,
    /// See [`SegmentIndex::log_start`].
    pub log_start: Option<Vec<u8>>,
    /// For a transcript segment, one entry per record: how many messages the conversation had
    /// placed before it. Blocks record the ordinal their first message gets from it. Empty when
    /// the caller doesn't track ordinals.
    pub placed_before: Vec<u64>,
}

/// Splits transcript records into groups that must share a block: each user input or assistant
/// message starts a group, and the records after it (its tool results, text delivered with a tool
/// result, markers, entry facts) join it. Ledger records are grouped one by one.
///
/// A message whose `EntryFacts` hasn't come yet keeps its group open. Messages placed together,
/// such as a run's several input messages, are written before all their facts, and a block
/// holding a message without its facts would read it without them. Every message of a session
/// that records facts gets them in the batch that places it, so a segment holding any
/// `EntryFacts` holds every message's; one holding none (a session from before facts) groups as
/// before.
pub fn groups(records: &[(u64, Record)]) -> Vec<Range<usize>> {
    let mut out: Vec<Range<usize>> = Vec::new();
    let tracks_facts = records
        .iter()
        .any(|(_, record)| matches!(record, Record::EntryFacts(_)));
    // Messages in the current group still waiting for their `EntryFacts`.
    let mut owed = 0usize;
    for (index, (_, record)) in records.iter().enumerate() {
        let starts = match record {
            Record::Input(_) | Record::Assistant(_) => {
                let starts = owed == 0;
                owed += usize::from(tracks_facts);
                starts
            }
            Record::EntryFacts(_) => {
                owed = owed.saturating_sub(1);
                false
            }
            Record::ToolReply(_)
            | Record::ResultAppend(_)
            | Record::Marker(_)
            | Record::SetupStub(_)
            | Record::Compaction(_) => false,
            _ => true,
        };
        match out.last_mut() {
            Some(last) if !starts => last.end = index + 1,
            _ => out.push(index..index + 1),
        }
    }
    out
}

/// Builds a segment of `records`, all of one stream and in increasing seq order. `name` is the
/// object's full name, which every reader must pass.
pub fn build(
    key: &FrameKey,
    name: &[u8],
    records: &[(u64, Record)],
    meta: SegmentMeta,
) -> Result<Vec<u8>, SegmentError> {
    let first = records.first().ok_or(SegmentError::BadRecords(
        "a segment holds at least one record",
    ))?;
    let stream = first.1.stream();
    if records.iter().any(|(_, record)| record.stream() != stream) {
        return Err(SegmentError::BadRecords("records of both streams"));
    }
    if records.windows(2).any(|pair| pair[1].0 <= pair[0].0) {
        return Err(SegmentError::BadRecords("seqs not increasing"));
    }
    if !meta.placed_before.is_empty() && meta.placed_before.len() != records.len() {
        return Err(SegmentError::BadRecords("one placed count per record"));
    }
    let header = SegmentHeader {
        format: u32::from(SEGMENT_FORMAT),
        session_id: meta.session_id,
        stream: stream_code(stream),
        first_seq: first.0,
        last_seq: records.last().expect("non-empty").0,
        created_at_ms: meta.created_at_ms,
        prefixes: meta.prefixes,
    }
    .encode();

    let mut out = Vec::new();
    out.extend_from_slice(SEGMENT_MAGIC);
    out.push(SEGMENT_FORMAT);
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&header);

    let ranges = match stream {
        Stream::Transcript => groups(records),
        Stream::Ledger => (0..records.len()).map(|i| i..i + 1).collect(),
    };
    let stored: Vec<StoredRecord> = records
        .iter()
        .map(|(seq, record)| StoredRecord {
            seq: *seq,
            kind: record.kind(),
            body: ByteBuf(record.encode_body()),
        })
        .collect();
    let placed_before = meta.placed_before;
    let mut entries = Vec::new();
    let mut block: Vec<StoredRecord> = Vec::new();
    let mut block_bytes = 0usize;
    // Index of the current block's first record in `records`.
    let mut block_first = 0usize;
    let mut flush = |block: &mut Vec<StoredRecord>,
                     block_bytes: &mut usize,
                     block_first: usize,
                     out: &mut Vec<u8>|
     -> Result<(), SegmentError> {
        if block.is_empty() {
            return Ok(());
        }
        let first_seq = block[0].seq;
        let first_ordinal = placed_before.get(block_first).map(|placed| placed + 1);
        let count = block.len() as u64;
        let plain = BlockBody {
            records: std::mem::take(block),
        }
        .encode();
        let compressed = zstd::bulk::compress(&plain, ZSTD_LEVEL)
            .map_err(|e| SegmentError::Compression(e.to_string()))?;
        let position = entries.len() as u32;
        let sealed = key.seal_unit(&unit_aad(&header, key, name, position), &compressed);
        entries.push(BlockEntry {
            first_seq,
            offset: out.len() as u64,
            len: sealed.len() as u64,
            records: count,
            first_ordinal,
        });
        out.extend_from_slice(&sealed);
        *block_bytes = 0;
        Ok(())
    };
    for range in ranges {
        let group_bytes: usize = stored[range.clone()]
            .iter()
            .map(|record| record.body.0.len() + 16)
            .sum();
        if !block.is_empty() && block_bytes + group_bytes > BLOCK_TARGET {
            flush(&mut block, &mut block_bytes, block_first, &mut out)?;
        }
        if block.is_empty() {
            block_first = range.start;
        }
        block.extend(stored[range].iter().cloned());
        block_bytes += group_bytes;
        if block_bytes >= BLOCK_TARGET {
            flush(&mut block, &mut block_bytes, block_first, &mut out)?;
        }
    }
    flush(&mut block, &mut block_bytes, block_first, &mut out)?;

    let index = SegmentIndex {
        blocks: entries,
        leading: meta.leading.map(ByteBuf),
        trailer: meta.trailer.map(ByteBuf),
        log_start: meta.log_start.map(ByteBuf),
    }
    .encode();
    let index_offset = out.len() as u64;
    let sealed = key.seal_unit(&unit_aad(&header, key, name, INDEX_POSITION), &index);
    out.extend_from_slice(&sealed);
    out.extend_from_slice(&index_offset.to_le_bytes());
    out.extend_from_slice(&(sealed.len() as u32).to_le_bytes());
    out.extend_from_slice(SEGMENT_MAGIC);
    Ok(out)
}

/// The `len` bytes at `offset` in `bytes`, when they lie inside it. Offsets and lengths come from
/// stored bytes, so they are checked, never added unchecked.
fn range(bytes: &[u8], offset: u64, len: u64) -> Option<&[u8]> {
    let start = usize::try_from(offset).ok()?;
    let end = start.checked_add(usize::try_from(len).ok()?)?;
    bytes.get(start..end)
}

/// A segment opened for reading: its header and index, decrypted. Blocks are read on demand.
pub struct Segment<'a> {
    bytes: &'a [u8],
    header_bytes: Vec<u8>,
    pub header: SegmentHeader,
    pub index: SegmentIndex,
    key: &'a FrameKey,
    name: &'a [u8],
}

impl<'a> Segment<'a> {
    /// Opens a segment from its bytes under `name` and `key`. Reads the header and the index,
    /// never a block.
    pub fn open(bytes: &'a [u8], key: &'a FrameKey, name: &'a [u8]) -> Result<Self, SegmentError> {
        if bytes.len() < 9 + FOOTER_LEN || &bytes[..4] != SEGMENT_MAGIC {
            return Err(SegmentError::Malformed("not a segment"));
        }
        if bytes[4] != SEGMENT_FORMAT {
            return Err(SegmentError::Malformed("unknown segment format"));
        }
        let header_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let header_bytes = 9usize
            .checked_add(header_len)
            .and_then(|end| bytes.get(9..end))
            .ok_or(SegmentError::Malformed("header past the end"))?
            .to_vec();
        let header = SegmentHeader::decode(&header_bytes).map_err(SegmentError::Decode)?;
        let footer = &bytes[bytes.len() - FOOTER_LEN..];
        if &footer[12..] != SEGMENT_MAGIC {
            return Err(SegmentError::Malformed("no footer"));
        }
        let index_offset = u64::from_le_bytes(footer[..8].try_into().unwrap());
        let index_len = u32::from_le_bytes(footer[8..12].try_into().unwrap());
        let sealed = range(bytes, index_offset, u64::from(index_len))
            .ok_or(SegmentError::Malformed("index past the end"))?;
        let index = key
            .open_unit(&unit_aad(&header_bytes, key, name, INDEX_POSITION), sealed)
            .ok_or(SegmentError::Authentication)?;
        let index = SegmentIndex::decode(&index).map_err(SegmentError::Decode)?;
        Ok(Self {
            bytes,
            header_bytes,
            header,
            index,
            key,
            name,
        })
    }

    /// The records of block `position`.
    pub fn block(&self, position: usize) -> Result<Vec<(u64, Record)>, SegmentError> {
        let entry = self
            .index
            .blocks
            .get(position)
            .ok_or(SegmentError::Malformed("no such block"))?;
        let sealed = range(self.bytes, entry.offset, entry.len)
            .ok_or(SegmentError::Malformed("block past the end"))?;
        let compressed = self
            .key
            .open_unit(
                &unit_aad(&self.header_bytes, self.key, self.name, position as u32),
                sealed,
            )
            .ok_or(SegmentError::Authentication)?;
        // The writer's frames state their decompressed size, so a block allocates what it holds;
        // the largest a block may hold, allocated for every read, would cost 64 MiB a block.
        let size = match zstd::zstd_safe::get_frame_content_size(&compressed) {
            Ok(Some(size)) if size <= MAX_BLOCK as u64 => size as usize,
            _ => {
                return Err(SegmentError::Compression(
                    "a block's frame doesn't state a size within the limit".into(),
                ))
            }
        };
        let plain = zstd::bulk::decompress(&compressed, size)
            .map_err(|e| SegmentError::Compression(e.to_string()))?;
        if plain.len() != size {
            return Err(SegmentError::Compression(
                "a block decompressed to another size than its frame states".into(),
            ));
        }
        let body = BlockBody::decode(&plain).map_err(SegmentError::Decode)?;
        if body.records.len() as u64 != entry.records
            || body.records.first().map(|r| r.seq) != Some(entry.first_seq)
        {
            return Err(SegmentError::Malformed(
                "block disagrees with its index entry",
            ));
        }
        body.records
            .into_iter()
            .map(|stored| {
                Record::decode(stored.kind, &stored.body.0)
                    .map(|record| (stored.seq, record))
                    .map_err(SegmentError::Decode)
            })
            .collect()
    }

    /// The block whose messages include `ordinal`, in a transcript segment built with placed
    /// counts: the last block whose first message's ordinal is at most `ordinal`. A block that
    /// places no message shares its first ordinal with the next, which is where the message is.
    pub fn block_for_ordinal(&self, ordinal: u64) -> Option<usize> {
        let blocks = &self.index.blocks;
        if blocks.iter().any(|entry| entry.first_ordinal.is_none()) {
            return None;
        }
        blocks
            .partition_point(|entry| entry.first_ordinal.is_some_and(|first| first <= ordinal))
            .checked_sub(1)
    }

    /// The record with `seq`, read from the one block that can hold it, or `None` when the
    /// segment has no such record.
    pub fn find(&self, seq: u64) -> Result<Option<Record>, SegmentError> {
        let position = self
            .index
            .blocks
            .partition_point(|entry| entry.first_seq <= seq);
        if position == 0 {
            return Ok(None);
        }
        Ok(self
            .block(position - 1)?
            .into_iter()
            .find(|(found, _)| *found == seq)
            .map(|(_, record)| record))
    }
}

#[cfg(test)]
mod tests;
