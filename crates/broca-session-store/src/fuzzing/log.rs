//! Open-log reader soundness: a log written from known batches, changed in one place, and the
//! verdicts the reader and the damage analysis must give for it.
//!
//! What must hold, from the `log` and `damage` module docs:
//! - the reader returns only records that were written, and only whole batches of them;
//! - a log cut short reads as a torn tail at the start of the first batch it cuts, or as clean
//!   when the cut falls between batches, but never a log cut inside its checkpoint (an empty one
//!   included), whose acknowledged bytes are gone;
//! - a changed byte reads as damage at the frame holding it, never as clean or a torn tail;
//!   below the durable bound it is corruption, unless only the checkpoint was hit, which
//!   recovery rebuilds;
//! - the durable bound is the largest sync claim of the frames that still authenticate, or the
//!   length synced at creation.

use std::ops::Range;

use super::{chain_base, frame_key, small_record, Input, BIND, KEY_ID};
use crate::checkpoint::Checkpoint;
use crate::codec::Message as _;
use crate::damage::{self, Lost, Verdict};
use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;
use crate::frame::{self, FrameHeader, FrameStream, FLAG_CLAIM_ONLY};
use crate::keys::Cipher;
use crate::log::{self, ChainBase, LogEncoder, LogEnd, ReadLog};
use crate::record::{Record, Stream};

/// One step of writing a log.
#[derive(Debug, Clone)]
pub enum LogOp {
    /// Records appended as one batch.
    Batch(Vec<Record>),
    /// A barrier: what was written so far is durable, and later frames claim it.
    Barrier,
    /// A claim-only frame.
    Claim,
}

/// One change to a written log.
#[derive(Debug, Clone, Copy)]
pub enum Mutation {
    None,
    /// Cut the log to its first `n` bytes.
    Truncate(usize),
    /// XOR one byte with a nonzero mask.
    Xor {
        at: usize,
        mask: u8,
    },
    /// Overwrite up to four bytes from `at`: a burst a CRC32C always detects in a header.
    Replace {
        at: usize,
        len: usize,
        with: [u8; 4],
    },
}

/// What a written frame is part of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Checkpoint,
    Claim,
    Batch(usize),
}

#[derive(Debug, Clone)]
pub struct WrittenFrame {
    pub range: Range<usize>,
    /// The sync claim the frame carries.
    pub claim: u64,
    unit: Unit,
    stream: Option<Stream>,
}

#[derive(Debug, Clone)]
pub struct WrittenBatch {
    pub range: Range<usize>,
    pub records: Vec<(u64, Record)>,
}

/// A log as written: its bytes, each frame and batch in it, the length synced at creation, and
/// the mode its frames are sealed in.
#[derive(Debug, Clone)]
pub struct WrittenLog {
    pub bytes: Vec<u8>,
    pub created_len: u64,
    pub frames: Vec<WrittenFrame>,
    pub batches: Vec<WrittenBatch>,
    pub cipher: Cipher,
}

impl WrittenLog {
    fn add(&mut self, bytes: &[u8], claim: u64, unit: Unit, streams: &[Option<Stream>]) {
        let mut at = 0;
        let mut position = 0;
        while at < bytes.len() {
            let len = 4 + u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            let start = self.bytes.len() + at;
            self.frames.push(WrittenFrame {
                range: start..start + len,
                claim,
                unit,
                stream: streams.get(position).copied().flatten(),
            });
            at += len;
            position += 1;
        }
        self.bytes.extend_from_slice(bytes);
    }

    /// The byte ranges of the log's checkpoint, claim-only frames and batches, in order.
    fn units(&self) -> Vec<Range<usize>> {
        let mut units: Vec<Range<usize>> = self
            .frames
            .iter()
            .filter(|frame| !matches!(frame.unit, Unit::Batch(_)))
            .map(|frame| frame.range.clone())
            .chain(self.batches.iter().map(|batch| batch.range.clone()))
            .collect();
        units.sort_by_key(|unit| unit.start);
        units
    }

    fn records_before(&self, end: usize) -> Vec<(u64, Record)> {
        self.batches
            .iter()
            .filter(|batch| batch.range.end <= end)
            .flat_map(|batch| batch.records.iter().cloned())
            .collect()
    }

    /// The end of the last whole unit at or before `at`.
    fn whole_through(&self, at: usize) -> usize {
        self.units()
            .iter()
            .map(|unit| unit.end)
            .filter(|end| *end <= at)
            .max()
            .unwrap_or(0)
    }

    fn bound<'a>(&self, frames: impl Iterator<Item = &'a WrittenFrame>) -> u64 {
        frames
            .map(|frame| frame.claim)
            .fold(self.created_len, u64::max)
    }
}

/// Writes a log whose records start at `next_seq` after the records `base` names, then runs
/// `ops`, as `LogWriter` and `session` write logs, its frames sealed in mode `cipher`.
pub fn write_log(cipher: Cipher, next_seq: u64, base: ChainBase, ops: &[LogOp]) -> WrittenLog {
    let mut encoder = LogEncoder::new(frame_key(cipher), BIND.to_vec(), next_seq, base);
    let checkpoint = Checkpoint {
        covers_through: 0,
        next_seq,
        conversation: Conversation::new(false),
        operational: Operations::default(),
        transcript_base: base.transcript,
        ledger_base: base.ledger,
    }
    .encode();
    let mut log = WrittenLog {
        bytes: Vec::new(),
        created_len: 0,
        frames: Vec::new(),
        batches: Vec::new(),
        cipher,
    };
    let encoded = encoder
        .checkpoint(&checkpoint)
        .expect("a new log's checkpoint");
    log.add(&encoded.bytes, 0, Unit::Checkpoint, &[]);
    encoder.written(&encoded);
    encoder.synced();
    let mut synced = encoder.offset();
    log.created_len = synced;
    for op in ops {
        match op {
            LogOp::Batch(records) => {
                let Ok(encoded) = encoder.batch(records) else {
                    continue;
                };
                let first = encoded.first_seq.expect("a batch takes seqs");
                let start = log.bytes.len();
                let streams: Vec<_> = records.iter().map(|record| Some(record.stream())).collect();
                log.add(
                    &encoded.bytes,
                    synced,
                    Unit::Batch(log.batches.len()),
                    &streams,
                );
                log.batches.push(WrittenBatch {
                    range: start..log.bytes.len(),
                    records: (first..).zip(records.iter().cloned()).collect(),
                });
                encoder.written(&encoded);
            }
            LogOp::Barrier => {
                encoder.synced();
                synced = encoder.offset();
            }
            LogOp::Claim => {
                let encoded = encoder.claim().expect("a claim-only frame");
                log.add(&encoded.bytes, synced, Unit::Claim, &[]);
                encoder.written(&encoded);
            }
        }
    }
    log
}

/// A log in mode `cipher` and one change to it, chosen by the input.
pub fn from_input(input: &mut Input<'_>, cipher: Cipher) -> (WrittenLog, Mutation) {
    let next_seq = 1 + input.number() % 1000;
    let base = ChainBase {
        transcript: (input.byte() & 1 == 1).then(|| input.number() % next_seq),
        ledger: (input.byte() & 1 == 1).then(|| input.number() % next_seq),
    };
    let mut ops = Vec::new();
    for _ in 0..input.byte() % 12 {
        ops.push(match input.byte() % 4 {
            0 | 1 => LogOp::Batch(
                (0..1 + input.byte() % 4)
                    .map(|_| small_record(input))
                    .collect(),
            ),
            2 => LogOp::Barrier,
            _ => LogOp::Claim,
        });
    }
    let log = write_log(cipher, next_seq, base, &ops);
    let len = log.bytes.len();
    let at = usize::from(input.u16()) % len;
    let mutation = match input.byte() % 4 {
        0 => Mutation::None,
        1 => Mutation::Truncate(at),
        2 => Mutation::Xor {
            at,
            mask: input.byte().max(1),
        },
        _ => Mutation::Replace {
            at,
            len: 1 + usize::from(input.byte() % 4),
            with: [input.byte(), input.byte(), input.byte(), input.byte()],
        },
    };
    (log, mutation)
}

/// Applies `mutation`: the changed bytes, and the range of bytes that differ, if any.
fn apply(log: &WrittenLog, mutation: Mutation) -> (Vec<u8>, Option<Range<usize>>) {
    let mut bytes = log.bytes.clone();
    match mutation {
        Mutation::None => (bytes, None),
        Mutation::Truncate(at) => {
            bytes.truncate(at);
            (bytes, None)
        }
        Mutation::Xor { at, mask } => {
            bytes[at] ^= mask;
            (bytes, (mask != 0).then_some(at..at + 1))
        }
        Mutation::Replace { at, len, with } => {
            let mut changed: Option<Range<usize>> = None;
            for (offset, byte) in with.iter().take(len).enumerate() {
                let Some(slot) = bytes.get_mut(at + offset) else {
                    break;
                };
                if *slot != *byte {
                    *slot = *byte;
                    let position = at + offset;
                    changed = Some(match changed {
                        Some(range) => range.start..position + 1,
                        None => position..position + 1,
                    });
                }
            }
            (bytes, changed)
        }
    }
}

/// Checks what holds of any log read, however it was written: the reader's end, the records it
/// returned and its batches agree with each other.
pub fn check_read_shape(len: u64, created_len: u64, read: &ReadLog) {
    assert!(read.complete_through <= len);
    match &read.end {
        LogEnd::Clean => assert_eq!(read.complete_through, len, "a clean log ends early"),
        LogEnd::TornTail { at } => assert_eq!(
            *at, read.complete_through,
            "a torn tail starts away from the end of the complete batches"
        ),
        LogEnd::Damaged { at, .. } => {
            assert!(
                read.complete_through <= *at && *at < len,
                "damage outside the log"
            )
        }
    }
    assert!(
        read.records.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "records whose seqs don't increase"
    );
    let mut next = 0;
    for batch in &read.batches {
        assert!(
            batch.start == next && batch.end > batch.start,
            "batches that don't cover the records in order"
        );
        next = batch.end;
    }
    assert_eq!(next, read.records.len(), "records outside every batch");
    assert!(read.durable_bound >= created_len);
}

/// Reads `log` changed by `mutation`, in the mode it was written in, and checks every verdict
/// against what was written.
pub fn check_log(log: &WrittenLog, mutation: &Mutation) {
    let key = frame_key(log.cipher);
    assert_eq!(key.id(), KEY_ID);
    let (bytes, changed) = apply(log, *mutation);
    let len = bytes.len();
    let read = log::read(&bytes, &key, BIND, log.created_len, chain_base)
        .expect("a log whose frames all decode reads");
    check_read_shape(len as u64, log.created_len, &read);
    let verdict = damage::analyse(&bytes, &key, BIND, log.created_len, chain_base, None)
        .expect("a log whose frames all decode is analysed");

    if let Some(changed) = changed {
        check_changed(log, &read, &verdict, changed);
        return;
    }
    // Unchanged, or cut short at `len`.
    let whole = log.whole_through(len);
    assert_eq!(
        read.records,
        log.records_before(len),
        "a log cut at {len} read other records than its whole batches"
    );
    let expected_end = if whole == len && len > 0 {
        LogEnd::Clean
    } else {
        LogEnd::TornTail { at: whole as u64 }
    };
    assert_eq!(
        read.end,
        expected_end,
        "a log cut at {len} of {}",
        log.bytes.len()
    );
    assert_eq!(read.complete_through, whole as u64);
    let bound = log.bound(log.frames.iter().filter(|frame| frame.range.end <= len));
    assert_eq!(
        read.durable_bound, bound,
        "the durable bound of a log cut at {len}"
    );
    match (&read.end, &verdict) {
        (LogEnd::Clean, Verdict::Opens(_)) => {}
        (LogEnd::TornTail { at }, Verdict::Opens(_)) if *at >= bound => {}
        (
            LogEnd::TornTail { at },
            Verdict::Corrupt {
                at: corrupt,
                durable_bound,
            },
        ) if at < &bound && corrupt == at && *durable_bound == bound => {}
        (end, verdict) => panic!(
            "a log cut at {len} with durable bound {bound} reads as {end:?} and is analysed as {}",
            describe(verdict)
        ),
    }
}

fn check_changed(log: &WrittenLog, read: &ReadLog, verdict: &Verdict, changed: Range<usize>) {
    let damaged: Vec<&WrittenFrame> = log
        .frames
        .iter()
        .filter(|frame| frame.range.start < changed.end && changed.start < frame.range.end)
        .collect();
    let first = damaged[0];
    let at = first.range.start;
    match &read.end {
        LogEnd::Damaged { at: found, .. } => assert_eq!(
            *found, at as u64,
            "a change at {changed:?} reads as damage at another frame"
        ),
        end => panic!("a change at {changed:?} reads as {end:?}, not as damage"),
    }
    assert_eq!(
        read.records,
        log.records_before(at),
        "a change at {changed:?} read other records than the whole batches before it"
    );
    assert_eq!(read.complete_through, log.whole_through(at) as u64);

    let intact = || {
        log.frames
            .iter()
            .filter(|frame| !damaged.iter().any(|hit| hit.range == frame.range))
    };
    let bound = log.bound(intact());
    let checkpoint_only =
        damaged.len() == 1 && first.unit == Unit::Checkpoint && log.frames.len() > 1;
    match verdict {
        Verdict::Corrupt {
            at: corrupt,
            durable_bound,
        } if (at as u64) < bound && !checkpoint_only => {
            assert_eq!((*corrupt, *durable_bound), (at as u64, bound));
        }
        Verdict::Debris(debris) if at as u64 >= bound || checkpoint_only => {
            assert_eq!(debris.durable_bound, bound);
            assert_eq!(debris.at(), at as u64);
            assert_eq!(debris.checkpoint.is_some(), first.unit != Unit::Checkpoint);
            let harvested: Vec<(u64, u64)> = debris
                .harvested
                .iter()
                .map(|frame| (frame.at, frame.len))
                .collect();
            let expected: Vec<(u64, u64)> = intact()
                .map(|frame| (frame.range.start as u64, frame.range.len() as u64))
                .collect();
            assert_eq!(
                harvested, expected,
                "harvested frames aren't the intact ones"
            );
            let written: Vec<&(u64, Record)> = log
                .batches
                .iter()
                .flat_map(|batch| batch.records.iter())
                .collect();
            for frame in &debris.harvested {
                if let Some(record) = &frame.record {
                    assert!(
                        written.contains(&&(frame.header.seq, record.clone())),
                        "harvested a record that wasn't written"
                    );
                }
            }
            // A region says a stream lost nothing only when no damaged frame held its records.
            for region in &debris.regions {
                for (stream, lost) in [
                    (Stream::Transcript, region.transcript),
                    (Stream::Ledger, region.ledger),
                ] {
                    let hit = damaged.iter().any(|frame| {
                        frame.stream == Some(stream)
                            && (frame.range.start as u64)
                                < region.bytes.end.max(region.bytes.start + 1)
                            && region.bytes.start < frame.range.end as u64
                    });
                    assert!(
                        !(hit && lost == Lost::Nothing),
                        "a region claims {stream:?} lost nothing where a damaged frame held one"
                    );
                }
            }
        }
        verdict => panic!(
            "a change at {changed:?} (frame at {at}, durable bound {bound}) is analysed as {}",
            describe(verdict)
        ),
    }
}

fn describe(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Opens(read) => format!("opening, ending {:?}", read.end),
        Verdict::Corrupt { at, durable_bound } => {
            format!("corrupt at {at} below {durable_bound}")
        }
        Verdict::Debris(debris) => format!("debris from {}", debris.at()),
    }
}

/// Frames whose headers and bodies the input chooses, sealed under the test key in mode
/// `cipher`, possibly cut or changed after: what an authenticated but inconsistent log looks like
/// to the reader.
pub fn sealed_frames(input: &mut Input<'_>, cipher: Cipher) -> Vec<u8> {
    let key = frame_key(cipher);
    let mut out = Vec::new();
    let mut seq = input.number();
    let mut index = 0u32;
    let mut last = ChainBase::default();
    for position in 0..input.byte() % 16 {
        let control = input.byte();
        let checkpoint = position == 0 && control & 0x80 == 0;
        let claim = !checkpoint && control & 0x40 != 0;
        let (stream, kind, body) = if checkpoint {
            let body = if control & 0x20 == 0 {
                Checkpoint {
                    covers_through: input.number(),
                    next_seq: seq,
                    conversation: Conversation::new(false),
                    operational: Operations::default(),
                    transcript_base: (input.byte() & 1 == 1).then(|| input.number()),
                    ledger_base: (input.byte() & 1 == 1).then(|| input.number()),
                }
                .encode()
            } else {
                input.chunk().to_vec()
            };
            (FrameStream::None, 0, body)
        } else if claim {
            (FrameStream::None, 0, Vec::new())
        } else if control & 0x20 != 0 {
            let stream = match input.byte() % 3 {
                0 => FrameStream::None,
                1 => FrameStream::Transcript,
                _ => FrameStream::Ledger,
            };
            (stream, input.byte(), input.chunk().to_vec())
        } else {
            let record = small_record(input);
            let stream = match record.stream() {
                Stream::Transcript => FrameStream::Transcript,
                Stream::Ledger => FrameStream::Ledger,
            };
            (stream, record.kind() as u8, record.encode_body())
        };
        let record_seq = if stream == FrameStream::None {
            0
        } else if control & 0x08 != 0 {
            input.number()
        } else {
            seq
        };
        let prev = match stream {
            FrameStream::Transcript => last.transcript,
            FrameStream::Ledger => last.ledger,
            FrameStream::None => None,
        };
        let header = FrameHeader {
            flags: if control & 0x01 != 0 {
                input.byte()
            } else if claim {
                FLAG_CLAIM_ONLY
            } else {
                0
            },
            stream,
            kind,
            kind_version: if control & 0x04 != 0 { input.byte() } else { 1 },
            key_id: KEY_ID,
            seq: record_seq,
            batch_first: if control & 0x10 != 0 {
                record_seq.saturating_sub(u64::from(input.byte() % 4))
            } else {
                record_seq
            },
            batch_count: if control & 0x10 != 0 {
                u16::from(input.byte() % 4)
            } else {
                1
            },
            frame_index: if !checkpoint && control & 0x80 != 0 {
                u32::from(input.byte() % 16)
            } else {
                index
            },
            synced_through: input.number(),
            prev_in_stream: if control & 0x02 != 0 {
                (input.byte() & 1 == 1).then(|| input.number())
            } else {
                prev
            },
        };
        let Ok(sealed) = frame::seal(&header, &body, &key, BIND) else {
            continue;
        };
        out.extend_from_slice(&sealed);
        index += 1;
        if stream != FrameStream::None {
            seq = record_seq.saturating_add(1);
            match stream {
                FrameStream::Transcript => last.transcript = Some(record_seq),
                _ => last.ledger = Some(record_seq),
            }
        }
    }
    if !out.is_empty() {
        match input.byte() % 4 {
            1 => out.truncate(usize::from(input.u16()) % out.len()),
            2 => {
                let at = usize::from(input.u16()) % out.len();
                out[at] ^= input.byte().max(1);
            }
            3 => out.extend_from_slice(input.chunk()),
            _ => {}
        }
    }
    out
}
