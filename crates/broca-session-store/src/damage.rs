//! Reading a damaged open log: the part of crash recovery that decides what happened.
//!
//! [`log::read`] stops at the first frame it can't accept. That is enough for a clean log or a
//! torn tail, but recovery needs more: every frame that still authenticates, wherever it lies,
//! and an exact account of what the damage may have removed. [`analyse`] gives that, and returns
//! one of three verdicts:
//!
//! - **The log opens** when it is clean or ends in a torn tail. A torn tail is an append that
//!   never synced: either cut short, or left as zero-filled blocks a filesystem allocated after a
//!   power loss but never wrote. It must start at or after the durable bound, the largest sync
//!   claim any authenticated frame carries, because no barrier acknowledged it.
//! - **Corrupt** when damage lies before the durable bound, in bytes a barrier acknowledged.
//!   Nothing may be cut; the session refuses until an operator looks.
//! - **Debris** otherwise: damage at or after the durable bound, or a damaged checkpoint (which
//!   recovery rebuilds from the previous ledger segment). Recovery repairs it.
//!
//! For debris it also says, for each damaged region and each stream, whether a record may have
//! been lost there. Every record's header names the previous record of its stream, so when the
//! first record of a stream after a region names the last record of that stream before it,
//! nothing of that stream was in the region. Otherwise, including when no record of the stream
//! follows, the loss can't be ruled out.
//!
//! An authenticated frame whose record doesn't decode is never damage: the session's own writer
//! wrote it, so it is a record this build can't read. That refuses, wherever it lies.

use std::ops::Range;

use crate::frame::{self, FrameHeader, FrameKey, FrameRead, FrameStream, FLAG_CLAIM_ONLY};
use crate::log::{self, ChainBase, LogEnd, LogError, ReadLog};
use crate::record::{Record, Stream};

/// A frame that authenticated.
#[derive(Debug, Clone)]
pub struct Harvested {
    pub at: u64,
    pub len: u64,
    pub header: FrameHeader,
    /// The record, for a record frame; `None` for the checkpoint and claim-only frames.
    pub record: Option<Record>,
}

impl Harvested {
    fn stream(&self) -> Option<Stream> {
        match self.header.stream {
            FrameStream::Transcript => Some(Stream::Transcript),
            FrameStream::Ledger => Some(Stream::Ledger),
            FrameStream::None => None,
        }
    }

    fn end(&self) -> u64 {
        self.at + self.len
    }
}

/// Whether a stream may have lost a record in a region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lost {
    /// The stream's first record after the region links back to its last record before it.
    Nothing,
    /// Nothing proves the stream lost no record there.
    CantProve,
}

/// Damaged bytes, or a point between two intact frames where frames are missing (a gap in the
/// frame indexes, or a break the log reader found in the batch or seq order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    pub bytes: Range<u64>,
    pub transcript: Lost,
    pub ledger: Lost,
}

#[derive(Debug)]
pub enum Verdict {
    /// Clean, or ending in a torn tail at or after the durable bound. A torn tail's reader stops
    /// before it, and the writer truncates it when it opens the log.
    Opens(ReadLog),
    /// Damage at `at`, below the durable bound: bytes a barrier acknowledged.
    Corrupt {
        at: u64,
        durable_bound: u64,
    },
    Debris(Debris),
}

/// A damaged log recovery can repair.
#[derive(Debug)]
pub struct Debris {
    pub durable_bound: u64,
    /// The checkpoint's body, or `None` when the checkpoint itself is damaged.
    pub checkpoint: Option<Vec<u8>>,
    /// Every frame that authenticated, in file order, except a torn tail's.
    pub harvested: Vec<Harvested>,
    /// The damaged regions, in file order.
    pub regions: Vec<Region>,
    /// Where a torn tail after the last region starts. Its frames aren't harvested, and it never
    /// counts as a loss.
    pub torn_tail: Option<u64>,
}

impl Debris {
    /// The first damaged byte.
    pub fn at(&self) -> u64 {
        self.regions[0].bytes.start
    }

    /// Whether every region provably lost nothing of either stream.
    pub fn loss_free(&self) -> bool {
        self.regions
            .iter()
            .all(|region| region.transcript == Lost::Nothing && region.ledger == Lost::Nothing)
    }
}

/// Every frame that authenticates, found by testing each byte offset where the previous frame
/// didn't continue, and the byte ranges between them.
struct Scan {
    frames: Vec<Harvested>,
    checkpoint: Option<Vec<u8>>,
    /// Byte ranges no authenticated frame covers, in order.
    gaps: Vec<Range<u64>>,
    /// Whether the last gap, if it runs to the end of the file, is what an unsynced append
    /// leaves: a frame cut short, or zeros.
    trailing_benign: bool,
}

fn scan(bytes: &[u8], key: &FrameKey, bind: &[u8]) -> Result<Scan, LogError> {
    let mut out = Scan {
        frames: Vec::new(),
        checkpoint: None,
        gaps: Vec::new(),
        trailing_benign: false,
    };
    let mut offset = 0usize;
    // The start of the gap being read, and whether it began with a frame cut short.
    let mut gap: Option<(usize, bool)> = None;
    while offset < bytes.len() {
        match frame::open(&bytes[offset..], |id| (id == key.id()).then_some(key), bind) {
            Ok(FrameRead::Frame { header, body, len }) => {
                if let Some((start, _)) = gap.take() {
                    out.gaps.push(start as u64..offset as u64);
                }
                let record = if header.stream == FrameStream::None {
                    let checkpoint = offset == 0
                        && header.frame_index == 0
                        && header.flags & FLAG_CLAIM_ONLY == 0;
                    if checkpoint {
                        out.checkpoint = Some(body);
                    }
                    None
                } else {
                    Some(
                        Record::decode(u32::from(header.kind), &body).map_err(|error| {
                            LogError::Undecodable {
                                offset: offset as u64,
                                error,
                            }
                        })?,
                    )
                };
                out.frames.push(Harvested {
                    at: offset as u64,
                    len: len as u64,
                    header,
                    record,
                });
                offset += len;
            }
            Ok(FrameRead::Incomplete) => {
                gap.get_or_insert((offset, true));
                offset += 1;
            }
            Err(_) => {
                gap.get_or_insert((offset, false));
                offset += 1;
            }
        }
    }
    if let Some((start, cut_short)) = gap {
        out.gaps.push(start as u64..bytes.len() as u64);
        out.trailing_benign = cut_short || bytes[start..].iter().all(|byte| *byte == 0);
    }
    Ok(out)
}

/// Where a torn tail at the end of `frames` starts, given that the file's own bytes end at
/// `end`: the start of a batch the file ends inside, or `end`. The torn batch's frames are
/// removed from `frames`.
fn torn_tail(frames: &mut Vec<Harvested>, end: u64) -> u64 {
    // The record frames of the last batch, if they run unbroken to `end`.
    let mut first = frames.len();
    let mut next_at = end;
    while first > 0 {
        let frame = &frames[first - 1];
        let unbroken = frame.end() == next_at
            && frame.stream().is_some()
            && frames.get(first).is_none_or(|later| {
                later.header.frame_index == frame.header.frame_index + 1
                    && later.header.batch_first == frame.header.batch_first
            });
        if !unbroken {
            break;
        }
        next_at = frame.at;
        first -= 1;
        if frame.header.seq == frame.header.batch_first {
            break;
        }
    }
    let Some(start) = frames.get(first) else {
        return end;
    };
    let whole_batch_start = start.header.seq == start.header.batch_first;
    let incomplete = (frames.len() - first) < usize::from(start.header.batch_count);
    if whole_batch_start && incomplete {
        let at = start.at;
        frames.truncate(first);
        at
    } else {
        end
    }
}

/// Reads a log that may be damaged. `created_len` and `base` are as for [`log::read`].
/// `rebuilt_base` is the chain base recovery rebuilt from the previous ledger segment, used only
/// when the checkpoint itself is damaged; without it, a loss after a damaged checkpoint can't be
/// ruled out.
pub fn analyse(
    bytes: &[u8],
    key: &FrameKey,
    bind: &[u8],
    created_len: u64,
    base: impl Fn(&[u8]) -> Result<ChainBase, String>,
    rebuilt_base: Option<ChainBase>,
) -> Result<Verdict, LogError> {
    let mut read = log::read(bytes, key, bind, created_len, &base)?;
    let damaged_at = match read.end {
        LogEnd::Clean => return Ok(Verdict::Opens(read)),
        LogEnd::TornTail { at } => {
            let durable_bound = read.durable_bound;
            return Ok(if at >= durable_bound {
                Verdict::Opens(read)
            } else {
                Verdict::Corrupt { at, durable_bound }
            });
        }
        LogEnd::Damaged { at, .. } => at,
    };

    let Scan {
        mut frames,
        checkpoint,
        mut gaps,
        trailing_benign,
    } = scan(bytes, key, bind)?;
    let len = bytes.len() as u64;
    // Every authenticated frame's sync claim is true, a torn tail's included.
    let durable_bound = frames
        .iter()
        .map(|frame| frame.header.synced_through)
        .fold(created_len, u64::max);

    // A torn tail at the end: what an unsynced append leaves after the last intact frame, plus
    // the frames of a batch the file ends inside.
    let tail_end = match gaps.last() {
        Some(last) if last.end == len && trailing_benign => {
            let start = last.start;
            gaps.pop();
            start
        }
        Some(last) if last.end == len => len + 1,
        _ => len,
    };
    let torn = if tail_end <= len {
        let at = torn_tail(&mut frames, tail_end);
        (at < len).then_some(at)
    } else {
        None
    };
    if let Some(at) = torn.filter(|at| *at < durable_bound) {
        return Ok(Verdict::Corrupt { at, durable_bound });
    }

    // Frames missing between two intact frames that touch.
    let mut regions: Vec<Range<u64>> = gaps;
    for pair in frames.windows(2) {
        if pair[0].end() == pair[1].at
            && pair[1].header.frame_index != pair[0].header.frame_index + 1
        {
            regions.push(pair[1].at..pair[1].at);
        }
    }
    let starts_with_checkpoint = frames
        .first()
        .is_some_and(|first| first.at == 0 && first.header.frame_index == 0);
    if !starts_with_checkpoint && !regions.iter().any(|region| region.start == 0) {
        regions.push(0..0);
    }
    let explained = torn.is_some_and(|at| damaged_at >= at)
        || regions
            .iter()
            .any(|region| region.start <= damaged_at && damaged_at <= region.end);
    if !explained {
        // The log reader found a break in the batch or seq order between frames that all
        // authenticate: frames are missing there.
        regions.push(damaged_at..damaged_at);
    }
    regions.sort_by_key(|region| (region.start, region.end));

    if regions.is_empty() {
        // Only a torn tail: zero-filled blocks, which the log reader saw as a damaged header.
        if let Some(at) = torn.filter(|at| *at == read.complete_through) {
            read.end = LogEnd::TornTail { at };
            return Ok(Verdict::Opens(read));
        }
        regions.push(damaged_at..damaged_at);
    }

    for region in &regions {
        // The checkpoint alone is rebuilt from the previous ledger segment, wherever the
        // durable bound lies.
        let checkpoint_only = region.start == 0
            && frames
                .iter()
                .find(|frame| frame.at >= region.end)
                .is_some_and(|next| next.header.frame_index == 1);
        if region.start < durable_bound && !checkpoint_only {
            return Ok(Verdict::Corrupt {
                at: region.start,
                durable_bound,
            });
        }
    }

    let chain_base = match &checkpoint {
        Some(body) => Some(base(body).map_err(LogError::Checkpoint)?),
        None => rebuilt_base,
    };
    let lost = |region: &Range<u64>, stream: Stream| {
        // The last record of the stream before the region, or the chain base; the outer
        // `None` means it can't be known.
        let before = frames
            .iter()
            .rfind(|frame| frame.end() <= region.start && frame.stream() == Some(stream))
            .map(|frame| Some(frame.header.seq))
            .or_else(|| chain_base.map(|base| base.get(stream)));
        let after = frames
            .iter()
            .find(|frame| frame.at >= region.end && frame.stream() == Some(stream));
        match (before, after) {
            (Some(before), Some(after)) if after.header.prev_in_stream == before => Lost::Nothing,
            _ => Lost::CantProve,
        }
    };
    let regions = regions
        .into_iter()
        .map(|bytes| Region {
            transcript: lost(&bytes, Stream::Transcript),
            ledger: lost(&bytes, Stream::Ledger),
            bytes,
        })
        .collect();
    Ok(Verdict::Debris(Debris {
        durable_bound,
        checkpoint,
        harvested: frames,
        regions,
        torn_tail: torn,
    }))
}

#[cfg(test)]
mod tests;
