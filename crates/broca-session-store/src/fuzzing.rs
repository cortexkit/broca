//! Decode entry points shared by the fuzz targets in `fuzz/` and the tests that replay their
//! inputs, so a crash a fuzz run found stays fixed in the normal test suite.
//!
//! Each decoder family has one entry point taking arbitrary bytes. It runs every decoder of the
//! family on them and checks what the format promises of any input:
//! - a decoder returns `Ok` or `Err`: it never panics and never hangs;
//! - it holds memory in proportion to its input ([`bounded`]), so a length prefix larger than the
//!   input refuses before allocating;
//! - whatever it accepts encodes back to exactly the bytes it read, unless it skipped an unknown
//!   optional field ([`canonical`]): two byte strings that decode to one value mean the decoder
//!   accepted bytes no writer writes.
//!
//! Decoders behind authentication (frames, sealed units, heads) can't be reached by bytes the
//! fuzzer makes up, since it can't forge a tag. So each entry point's first byte selects a mode:
//! the raw bytes as stored, or bytes the entry point seals under fixed test keys first, so the
//! fuzzer reaches the decoder past the tag.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use broca_protocol::content::{ContentBlock, Message as ProtocolMessage, ToolOutput, ToolResult};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;

use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::checkpoint::Checkpoint;
use crate::codec::{self, hex, ByteBuf, DecodeError, Id128, Message};
use crate::directory::{self, DirEntry, Page, PageRef, Pages};
use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;
use crate::frame::FrameKey;
use crate::head::{self, Head};
use crate::idindex::{self, IndexPages, IndexRoot};
use crate::keys::{DataKey, DeploymentKey};
use crate::log::ChainBase;
use crate::record::{Event, Marker, Record, StepStart};
use crate::segment::{self, BlockBody, BlockEntry, SegmentHeader, SegmentIndex, StoredRecord};

mod alloc;
#[cfg(test)]
pub(crate) mod gen;
mod log;

pub use alloc::CountingAlloc;
pub use log::{check_log, write_log, LogOp, Mutation, WrittenLog};

/// The frame key every entry point seals and opens under.
pub const KEY_ID: u16 = 7;
pub const KEY: [u8; 32] = [0x42; 32];
/// What frames are bound to: a session id and a log name, as `session` binds them.
pub const BIND: &[u8] = b"fuzz-session-id-fuzz-log-name-00";
/// The session id sealed pages and segments name.
pub const SESSION: Id128 = Id128([0x11; 16]);
/// The object name sealed segments are read under.
pub const SEGMENT_NAME: &[u8] = b"fuzz/segment";
const DEPLOYMENT_ID: u16 = 3;
const DEPLOYMENT: [u8; 32] = [0x24; 32];
const DATA_KEY: [u8; 32] = [0x66; 32];

pub fn frame_key() -> FrameKey {
    FrameKey::new(KEY_ID, &KEY)
}

/// Bytes a decoder may hold per input byte. The largest structural blow-up is an empty nested
/// message decoding to a struct of a few hundred bytes (an `EntryState` from two bytes), doubled
/// while its vector grows; anything proportional to a length prefix instead of the input is far
/// above it for every input a fuzz run tries.
pub const ALLOC_PER_BYTE: usize = 512;
/// Bytes a decoder may hold whatever its input: keys, authenticated data, a page of scratch.
pub const ALLOC_BASE: usize = 1 << 20;

/// Runs `decode` on `input` bytes and checks the most it held live at once stayed within
/// [`ALLOC_PER_BYTE`] per input byte plus [`ALLOC_BASE`]. `extra` adds bytes the input
/// legitimately expands to, such as a compressed block's stated size.
pub fn bounded<T>(input: usize, extra: usize, decode: impl FnOnce() -> T) -> T {
    let outer = alloc::peak();
    let start = alloc::live();
    alloc::reset_peak();
    let out = decode();
    let held = usize::try_from(alloc::peak() - start).unwrap_or(0);
    alloc::raise_peak(outer);
    let limit = input
        .saturating_add(extra)
        .saturating_mul(ALLOC_PER_BYTE)
        .saturating_add(ALLOC_BASE);
    assert!(
        held <= limit,
        "decoding {input} bytes held {held} bytes at once, over the limit of {limit}"
    );
    out
}

/// Decodes `bytes` and, when they decode, checks they are the value's canonical encoding: the
/// value encodes back to exactly `bytes` unless the decode skipped an unknown optional field,
/// and its encoding decodes to a value that encodes the same again.
pub fn canonical<T>(
    bytes: &[u8],
    decode: impl Fn(&[u8]) -> Result<T, DecodeError>,
    encode: impl Fn(&T) -> Vec<u8>,
) -> Option<T> {
    let skipped = codec::skipped_unknown_fields();
    let value = bounded(bytes.len(), 0, || decode(bytes)).ok()?;
    let skipped_none = codec::skipped_unknown_fields() == skipped;
    let again = encode(&value);
    if skipped_none {
        assert!(
            again == bytes,
            "accepted bytes that aren't the canonical encoding of what they decode to\n  read:       {}\n  re-encoded: {}",
            hex(bytes),
            hex(&again)
        );
    }
    let back = decode(&again)
        .unwrap_or_else(|error| panic!("an encoding doesn't decode: {error}: {}", hex(&again)));
    assert!(
        encode(&back) == again,
        "a value's encoding decodes to a value that encodes differently: {}",
        hex(&again)
    );
    Some(value)
}

/// [`canonical`] for a message type.
pub fn message<M: Message>(bytes: &[u8]) -> Option<M> {
    canonical(bytes, M::decode, M::encode)
}

/// Reads a fuzz input piece by piece. Reading past its end gives zeros and empty slices, so
/// every input means something.
pub struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self(data)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn byte(&mut self) -> u8 {
        match self.0.split_first() {
            Some((&byte, rest)) => {
                self.0 = rest;
                byte
            }
            None => 0,
        }
    }

    pub fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.byte(), self.byte()])
    }

    pub fn u32(&mut self) -> u32 {
        u32::from_le_bytes([self.byte(), self.byte(), self.byte(), self.byte()])
    }

    pub fn u64(&mut self) -> u64 {
        u64::from(self.u32()) | (u64::from(self.u32()) << 32)
    }

    /// A number, small most of the time: small numbers are where seqs, counts and offsets
    /// agree with each other, and the extremes are where arithmetic overflows.
    pub fn number(&mut self) -> u64 {
        match self.byte() {
            n @ 0..=0xef => u64::from(n),
            0xf0..=0xfb => self.u64(),
            0xfc => u64::from(u32::MAX),
            0xfd => u64::MAX - 1,
            _ => u64::MAX,
        }
    }

    pub fn take(&mut self, len: usize) -> &'a [u8] {
        let (head, rest) = self.0.split_at(len.min(self.0.len()));
        self.0 = rest;
        head
    }

    /// A length-prefixed piece: one length byte, or two after a 0xff.
    pub fn chunk(&mut self) -> &'a [u8] {
        let len = match self.byte() {
            0xff => usize::from(self.u16()),
            n => usize::from(n),
        };
        self.take(len)
    }

    pub fn string(&mut self) -> String {
        String::from_utf8_lossy(self.chunk()).into_owned()
    }

    pub fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.0)
    }
}

/// Runs a future of the memory backend to completion. Its futures never wait, so polling once
/// finishes them; a future that does wait is a bug here, not a hang to sit through.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("a memory backend future waited"),
    }
}

/// The chain base a log's checkpoint names, as `session` reads it.
pub fn chain_base(body: &[u8]) -> Result<ChainBase, String> {
    let checkpoint = Checkpoint::decode(body).map_err(|e| e.to_string())?;
    Ok(ChainBase {
        transcript: checkpoint.transcript_base,
        ledger: checkpoint.ledger_base,
    })
}

/// A small record of either stream, built from the input: log and segment entry points need
/// records that decode, and what they hold doesn't matter to those readers.
pub fn small_record(input: &mut Input<'_>) -> Record {
    match input.byte() % 4 {
        0 => Record::StepStart(StepStart {
            run_id: input.string(),
            step_id: input.number(),
        }),
        1 => Record::Marker(Marker {
            view_version: input.number(),
            first_kept_mid: input.string(),
            first_kept_ordinal: input.number(),
            checkpoint: input.number(),
            harness: None,
            session: None,
        }),
        2 => {
            // A body the fuzzer wrote, when it decodes as the kind it names.
            let kind = u32::from(input.byte());
            let body = input.chunk();
            Record::decode(kind, body).unwrap_or_else(|_| {
                Record::Event(Event {
                    name: String::from_utf8_lossy(body).into_owned(),
                    run_id: None,
                    body: None,
                })
            })
        }
        _ => Record::Event(Event {
            name: input.string(),
            run_id: (input.byte() & 1 == 1).then(|| input.string()),
            body: None,
        }),
    }
}

/// Record bodies by kind. The first byte is the kind, or after 0xff the next four bytes are, so
/// kinds past 255 are tried too; the rest is the body. The content types records nest are
/// decoded from the same body on their own as well.
pub fn record(data: &[u8]) {
    let mut input = Input::new(data);
    let kind = match input.byte() {
        0xff => input.u32(),
        kind => u32::from(kind),
    };
    let body = input.rest();
    if let Some(record) = canonical(
        body,
        |bytes| Record::decode(kind, bytes),
        Record::encode_body,
    ) {
        assert_eq!(record.kind(), kind, "a body decoded as another kind");
        assert_eq!(crate::record::kind_stream(kind), Some(record.stream()));
    }
    message::<ProtocolMessage>(body);
    message::<ContentBlock>(body);
    message::<ToolResult>(body);
    message::<ToolOutput>(body);
    message::<Usage>(body);
    message::<ModelHandle>(body);
}

/// Frames and the open-log reader. Mode 0 reads the bytes as a log; mode 1 seals frames whose
/// headers and bodies the input chooses and reads them as a log; mode 2 writes a valid log of
/// batches the input chooses, changes it as the input says, and checks every verdict the reader
/// and the damage analysis give against what was written (see [`check_log`]).
pub fn frame_log(data: &[u8]) {
    let mut input = Input::new(data);
    match input.byte() % 3 {
        0 => {
            let bytes = input.rest();
            read_any_log(bytes, 0);
        }
        1 => {
            let created_len = input.number();
            let bytes = log::sealed_frames(&mut input);
            read_any_log(&bytes, created_len);
        }
        _ => {
            let (written, mutation) = log::from_input(&mut input);
            check_log(&written, &mutation);
        }
    }
}

/// Reads `bytes` as a log every way the crate does and checks what holds of any log.
pub fn read_any_log(bytes: &[u8], created_len: u64) {
    let key = frame_key();
    let _ = bounded(bytes.len(), 0, || {
        crate::frame::open(bytes, |id| (id == KEY_ID).then_some(&key), BIND)
    });
    if let Ok(read) = bounded(bytes.len(), 0, || {
        crate::log::read(bytes, &key, BIND, created_len, chain_base)
    }) {
        log::check_read_shape(bytes.len() as u64, created_len, &read);
    }
    let _ = bounded(bytes.len(), 0, || {
        crate::damage::analyse(bytes, &key, BIND, created_len, chain_base, None)
    });
}

/// Sealed segments. Mode 0 opens the bytes as a segment and decodes them as each segment
/// message; mode 1 builds a segment whose header, blocks and index the input chooses, sealed
/// under the test key; mode 2 builds a valid segment of records the input chooses, changes it
/// as the input says, and reads it back.
pub fn segment(data: &[u8]) {
    let mut input = Input::new(data);
    match input.byte() % 3 {
        0 => {
            let bytes = input.rest();
            message::<SegmentHeader>(bytes);
            message::<SegmentIndex>(bytes);
            message::<BlockBody>(bytes);
            read_segment(bytes);
        }
        1 => {
            let bytes = sealed_segment(&mut input);
            read_segment(&bytes);
        }
        _ => {
            // Records of one stream, in increasing seq order, as a segment holds them.
            let mut records: Vec<(u64, Record)> = Vec::new();
            let mut seq = input.number();
            for _ in 0..input.byte() % 24 {
                records.push((seq, small_record(&mut input)));
                seq = seq.saturating_add(1 + input.number() % 3);
            }
            let stream = records.first().map(|(_, record)| record.stream());
            records.retain(|(_, record)| Some(record.stream()) == stream);
            records.dedup_by_key(|(seq, _)| *seq);
            let meta = segment::SegmentMeta {
                session_id: SESSION,
                created_at_ms: input.number(),
                prefixes: Vec::new(),
                leading: (input.byte() & 1 == 1).then(|| input.chunk().to_vec()),
                trailer: (input.byte() & 1 == 1).then(|| input.chunk().to_vec()),
                log_start: None,
                placed_before: Vec::new(),
            };
            let key = frame_key();
            let Ok(mut bytes) = segment::build(&key, SEGMENT_NAME, &records, meta) else {
                return;
            };
            match input.byte() % 3 {
                0 => {
                    let opened = segment::Segment::open(&bytes, &key, SEGMENT_NAME)
                        .expect("a segment as built opens");
                    let mut read = Vec::new();
                    for position in 0..opened.index.blocks.len() {
                        read.extend(opened.block(position).expect("a block as built reads"));
                    }
                    assert_eq!(read, records, "a segment reads back other records");
                }
                1 => {
                    let at = usize::from(input.u16()) % bytes.len();
                    bytes.truncate(at);
                    read_segment(&bytes);
                }
                _ => {
                    let at = usize::from(input.u16()) % bytes.len();
                    bytes[at] ^= input.byte().max(1);
                    read_segment(&bytes);
                }
            }
        }
    }
}

/// Opens `bytes` as a segment and, when it opens, reads every block it lists and looks records
/// up in it.
fn read_segment(bytes: &[u8]) {
    let key = frame_key();
    let Ok(opened) = bounded(bytes.len(), 0, || {
        segment::Segment::open(bytes, &key, SEGMENT_NAME)
    }) else {
        return;
    };
    for position in 0..opened.index.blocks.len().min(16) {
        // A block legitimately expands to its compressed frame's stated size.
        let stated = block_stated_size(bytes, &opened.index.blocks[position], &key, position);
        if let Ok(records) = bounded(bytes.len(), stated, || opened.block(position)) {
            assert_eq!(
                records.len() as u64,
                opened.index.blocks[position].records,
                "a block read with another record count than its index entry states"
            );
        }
    }
    for seq in [
        0,
        1,
        opened.header.first_seq,
        opened.header.last_seq,
        u64::MAX,
    ] {
        let _ = opened.find(seq);
    }
    let _ = opened.block_for_ordinal(1);
}

/// The size a block's compressed frame states it decompresses to, capped at the largest a block
/// may be, or 0 when it can't be read.
fn block_stated_size(bytes: &[u8], entry: &BlockEntry, key: &FrameKey, position: usize) -> usize {
    let Some(header) = segment_header_bytes(bytes) else {
        return 0;
    };
    let start = usize::try_from(entry.offset).unwrap_or(usize::MAX);
    let end = start.saturating_add(usize::try_from(entry.len).unwrap_or(usize::MAX));
    let Some(sealed) = bytes.get(start..end) else {
        return 0;
    };
    let aad = segment::unit_aad(header, key, SEGMENT_NAME, position as u32);
    let Some(compressed) = key.open_unit(&aad, sealed) else {
        return 0;
    };
    match zstd::zstd_safe::get_frame_content_size(&compressed) {
        Ok(Some(size)) => usize::try_from(size.min(segment::MAX_BLOCK as u64)).unwrap_or(0),
        _ => 0,
    }
}

fn segment_header_bytes(bytes: &[u8]) -> Option<&[u8]> {
    let len = u32::from_le_bytes(bytes.get(5..9)?.try_into().ok()?) as usize;
    bytes.get(9..9usize.checked_add(len)?)
}

/// A segment whose header, blocks and index the input chooses, each either a valid encoding
/// with input-chosen fields or bytes as given, sealed where a segment's units are.
fn sealed_segment(input: &mut Input<'_>) -> Vec<u8> {
    let key = frame_key();
    let header = if input.byte() & 1 == 0 {
        input.chunk().to_vec()
    } else {
        SegmentHeader {
            format: u32::from(input.byte() % 3),
            session_id: SESSION,
            stream: u32::from(input.byte() % 3),
            first_seq: input.number(),
            last_seq: input.number(),
            created_at_ms: input.number(),
            prefixes: Vec::new(),
        }
        .encode()
    };
    let mut out = Vec::new();
    out.extend_from_slice(segment::SEGMENT_MAGIC);
    out.push(if input.byte() == 0xff {
        input.byte()
    } else {
        segment::SEGMENT_FORMAT
    });
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&header);
    let mut entries = Vec::new();
    for position in 0..input.byte() % 5 {
        let plain = match input.byte() % 3 {
            // The bytes as the compressed frame itself.
            0 => input.chunk().to_vec(),
            // The bytes compressed, as a block body.
            1 => zstd::bulk::compress(input.chunk(), 1).expect("compressing in memory"),
            // A block body of stored records.
            _ => {
                let mut records = Vec::new();
                for _ in 0..input.byte() % 6 {
                    let record = small_record(input);
                    records.push(StoredRecord {
                        seq: input.number(),
                        kind: record.kind(),
                        body: ByteBuf(record.encode_body()),
                    });
                }
                zstd::bulk::compress(&BlockBody { records }.encode(), 1)
                    .expect("compressing in memory")
            }
        };
        let aad = segment::unit_aad(&header, &key, SEGMENT_NAME, u32::from(position));
        let sealed = key.seal_unit(&aad, &plain);
        entries.push(BlockEntry {
            first_seq: input.number(),
            offset: if input.byte() == 0xff {
                input.number()
            } else {
                out.len() as u64
            },
            len: if input.byte() == 0xff {
                input.number()
            } else {
                sealed.len() as u64
            },
            records: input.number(),
            first_ordinal: (input.byte() & 1 == 1).then(|| input.number()),
        });
        out.extend_from_slice(&sealed);
    }
    let index = if input.byte() & 1 == 0 {
        input.chunk().to_vec()
    } else {
        SegmentIndex {
            blocks: entries,
            leading: None,
            trailer: None,
            log_start: (input.byte() & 1 == 1).then(|| ByteBuf(input.chunk().to_vec())),
        }
        .encode()
    };
    let aad = segment::unit_aad(&header, &key, SEGMENT_NAME, u32::MAX);
    let sealed = key.seal_unit(&aad, &index);
    let mut index_offset = out.len() as u64;
    let mut index_len = sealed.len() as u32;
    if input.byte() == 0xff {
        index_offset = input.number();
        index_len = input.u32();
    }
    out.extend_from_slice(&sealed);
    out.extend_from_slice(&index_offset.to_le_bytes());
    out.extend_from_slice(&index_len.to_le_bytes());
    out.extend_from_slice(segment::SEGMENT_MAGIC);
    out
}

/// Session heads and log checkpoints. Mode 0 decodes the bytes as a head, a checkpoint and each
/// message they nest, and opens them as a stored head; mode 1 takes a head the bytes decode to,
/// gives it a data key wrapped under the test deployment key, seals it and opens it again.
pub fn head_checkpoint(data: &[u8]) {
    let mut input = Input::new(data);
    let mode = input.byte() % 2;
    let bytes = input.rest();
    let deployment = DeploymentKey::new(DEPLOYMENT_ID, &DEPLOYMENT);
    let head = message::<Head>(bytes);
    message::<Checkpoint>(bytes);
    message::<Conversation>(bytes);
    message::<Operations>(bytes);
    message::<head::SessionTriple>(bytes);
    message::<head::PendingDeletion>(bytes);
    message::<head::Residence>(bytes);
    let _ = chain_base(bytes);
    let _ = bounded(bytes.len(), 0, || head::open(bytes, &deployment));
    if mode == 1 {
        if let Some(mut head) = head {
            let key = DataKey::from_bytes(1, DATA_KEY);
            head.format = head::HEAD_FORMAT;
            head.keys = vec![head::WrappedKey {
                id: 1,
                deployment: DEPLOYMENT_ID,
                wrapped: ByteBuf(crate::keys::wrap(&deployment, &head.session_id, &key)),
            }];
            let sealed = head::seal(&head, &key);
            let (opened, _) = head::open(&sealed, &deployment).expect("a sealed head opens");
            assert_eq!(opened, head);
        }
    }
}

/// Directory pages and id-index pages. Mode 0 decodes the bytes as every page and reference
/// message; mode 1 stores directory pages the input chooses, sealed under the test key, and
/// walks them every way a directory is walked; mode 2 does the same for an id index.
pub fn directory_idindex(data: &[u8]) {
    let mut input = Input::new(data);
    match input.byte() % 3 {
        0 => {
            let bytes = input.rest();
            message::<Page>(bytes);
            message::<PageRef>(bytes);
            message::<DirEntry>(bytes);
            message::<IndexRoot>(bytes);
            message::<idindex::RefPage>(bytes);
            message::<idindex::DataPage>(bytes);
            message::<idindex::Retired>(bytes);
            message::<idindex::Level>(bytes);
        }
        1 => walk_directory(&mut input),
        _ => walk_idindex(&mut input),
    }
}

/// How many objects a walked directory or index holds. Enough for a tree three levels deep
/// with a branch on each, and for a page to name itself or a page above it.
const OBJECTS: u8 = 8;

fn object_name(prefix: &str, input: &mut Input<'_>) -> String {
    format!("{prefix}{}", input.byte() % OBJECTS)
}

fn page_ref(input: &mut Input<'_>) -> PageRef {
    PageRef {
        object: object_name("p", input),
        level: u32::from(input.byte() % 4),
        first_seq: input.number(),
        last_seq: input.number(),
        count: input.byte().into(),
        first_ordinal: (input.byte() & 1 == 1).then(|| input.number()),
        last_ordinal: (input.byte() & 1 == 1).then(|| input.number()),
    }
}

fn dir_entry(input: &mut Input<'_>) -> DirEntry {
    DirEntry {
        object: object_name("s", input),
        first_seq: input.number(),
        last_seq: input.number(),
        size: input.number(),
        sealed_at_ms: input.number(),
        first_ordinal: (input.byte() & 1 == 1).then(|| input.number()),
        last_ordinal: (input.byte() & 1 == 1).then(|| input.number()),
    }
}

fn memory_backend() -> (MemoryBackend, SessionAddr) {
    let backend = MemoryBackend::default();
    let addr = SessionAddr("fuzz".into());
    block_on(backend.write_head(&addr, b"head", None, 1)).expect("a new head");
    (backend, addr)
}

fn walk_directory(input: &mut Input<'_>) {
    let (backend, addr) = memory_backend();
    let key = frame_key();
    for n in 0..input.byte() % (OBJECTS + 1) {
        let name = format!("p{n}");
        let plain = if input.byte() & 1 == 0 {
            input.chunk().to_vec()
        } else {
            let level = u32::from(input.byte() % 4);
            let mut page = Page {
                level,
                entries: Vec::new(),
                children: Vec::new(),
            };
            for _ in 0..input.byte() % 6 {
                if input.byte() & 1 == 0 {
                    page.entries.push(dir_entry(input));
                } else {
                    page.children.push(page_ref(input));
                }
            }
            page.encode()
        };
        let sealed = key.seal_unit(&directory::aad(&SESSION, &name), &plain);
        backend.set_object(&addr, &name, sealed);
    }
    let pages = Pages {
        backend: &backend,
        addr: &addr,
        session: SESSION,
        key: &key,
        fanout: 1 + usize::from(input.byte() % 4),
    };
    let root = page_ref(input);
    let seq = input.number();
    let ordinal = (input.byte() & 1 == 1).then(|| input.number());
    let _ = block_on(pages.read(&root));
    let _ = block_on(pages.find(Some(&root), seq));
    let _ = block_on(pages.find_before(Some(&root), seq));
    let _ = block_on(pages.find_ordinal(Some(&root), seq));
    let _ = block_on(pages.abandoned(Some(&root), seq));
    let _ = block_on(pages.truncate(Some(&root), seq, ordinal, "t", 1));
    let _ = block_on(pages.append(Some(&root), dir_entry(input), "a", 1));
}

fn id128(input: &mut Input<'_>) -> Id128 {
    // Mostly one of a few hashes, so lookups, ranges and filters meet each other.
    match input.byte() {
        n @ 0..=0xef => Id128([n % 8; 16]),
        _ => Id128(input.take(16).try_into().unwrap_or([0xee; 16])),
    }
}

fn walk_idindex(input: &mut Input<'_>) {
    let (backend, addr) = memory_backend();
    let key = frame_key();
    let pages = IndexPages {
        backend: &backend,
        addr: &addr,
        session: SESSION,
        key: &key,
    };
    let seal_raw = |name: &str, plain: &[u8]| {
        let sealed = key.seal_unit(&idindex::aad(&SESSION, name), plain);
        backend.set_object(&addr, name, sealed);
    };
    for n in 0..input.byte() % (OBJECTS + 1) {
        let name = format!("d{n}");
        if input.byte() & 1 == 0 {
            seal_raw(&name, input.chunk());
        } else {
            let mut entries: Vec<idindex::Entry> = (0..input.byte() % 8)
                .map(|_| idindex::Entry {
                    hash: id128(input),
                    seq: input.number(),
                })
                .collect();
            if input.byte() & 1 == 0 {
                entries.sort_unstable();
            }
            let _ = block_on(pages.write(&name, &idindex::DataPage { entries }, 1));
        }
    }
    for n in 0..input.byte() % (OBJECTS + 1) {
        let name = format!("r{n}");
        if input.byte() & 1 == 0 {
            seal_raw(&name, input.chunk());
        } else {
            let infos = (0..input.byte() % 4)
                .map(|_| idindex::PageInfo {
                    object: object_name("d", input),
                    first: id128(input),
                    last: id128(input),
                    count: input.byte().into(),
                    filter: ByteBuf(input.chunk().to_vec()),
                })
                .collect();
            let _ = block_on(pages.write(&name, &idindex::RefPage { pages: infos }, 1));
        }
    }
    let root = if input.byte() & 1 == 0 {
        IndexRoot::decode(input.chunk()).unwrap_or_default()
    } else {
        let ref_info = |input: &mut Input<'_>| idindex::RefInfo {
            object: object_name("r", input),
            first: id128(input),
            last: id128(input),
        };
        let mut levels = Vec::new();
        for _ in 0..input.byte() % 3 {
            let runs = (0..input.byte() % 18)
                .map(|_| idindex::Run {
                    entries: input.number(),
                    refs: (0..input.byte() % 3).map(|_| ref_info(input)).collect(),
                })
                .collect();
            let merge = (input.byte() & 1 == 1).then(|| idindex::Merge {
                inputs: u32::from(input.byte()),
                done: (0..input.byte() % 3).map(|_| ref_info(input)).collect(),
                written: input.number(),
                next: (input.byte() & 1 == 1).then(|| idindex::Entry {
                    hash: id128(input),
                    seq: input.number(),
                }),
            });
            levels.push(idindex::Level { runs, merge });
        }
        IndexRoot { levels }
    };
    let hash = id128(input);
    let _ = block_on(pages.lookup(&root, &hash));
    let _ = block_on(pages.objects(&root));
    let entries = (0..input.byte() % 8)
        .map(|_| idindex::Entry {
            hash: id128(input),
            seq: input.number(),
        })
        .collect();
    let _ = block_on(pages.update(root, entries, "u", 1));
    let retired: Vec<String> = (0..input.byte() % 4)
        .map(|_| object_name("r", input))
        .collect();
    let _ = block_on(pages.delete_retired(&retired, 1));
}

#[cfg(test)]
mod tests;
