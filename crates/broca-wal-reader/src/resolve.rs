//! Positioned, publication-bounded reads. No locks, leases, database, or writer dependency.

use std::io;

use sha2::{Digest, Sha256};

use crate::checkpoint::{feature_digest, Boundary, Checkpoint, Restart, VERSION};
use crate::{frame, proof::HeadProof, Error};

pub(crate) const LINEAGE_LEN: u64 = 69;

/// A caller can instrument reads without changing the billing fold. File implementations do not
/// change the file cursor, and use the caller's already-open, share-delete-capable handle.
pub trait ReadAt {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize>;
}

impl ReadAt for std::fs::File {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(unix)]
        {
            std::os::unix::fs::FileExt::read_at(self, buffer, offset)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::FileExt::seek_read(self, buffer, offset)
        }
    }
}

impl ReadAt for [u8] {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        let tail = usize::try_from(offset)
            .ok()
            .and_then(|offset| self.get(offset..))
            .unwrap_or_default();
        let len = buffer.len().min(tail.len());
        buffer[..len].copy_from_slice(&tail[..len]);
        Ok(len)
    }
}

pub(crate) fn bytes(source: &(impl ReadAt + ?Sized), offset: u64, len: u64) -> io::Result<Vec<u8>> {
    let len = usize::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL range exceeds address space",
        )
    })?;
    let mut out = vec![0; len];
    let mut done = 0;
    while done < len {
        match source.read_at(&mut out[done..], offset + done as u64) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

pub(crate) fn lineage(raw: &[u8]) -> Option<[u8; 16]> {
    if raw.len() != LINEAGE_LEN as usize
        || raw[..4] != 16u32.to_le_bytes()
        || raw[4] != 2
        || raw[5..21] != [0; 16]
    {
        return None;
    }
    let mut hash = Sha256::new();
    hash.update(&raw[4..21]);
    hash.update(&raw[53..]);
    if hash.finalize().as_slice() != &raw[21..53] {
        return None;
    }
    raw[53..].try_into().ok()
}

pub(crate) fn validate(
    source: &(impl ReadAt + ?Sized),
    head: &HeadProof,
    cp: &Checkpoint,
) -> Result<(), Restart> {
    if cp.version != VERSION {
        return Err(Restart::StateVersion);
    }
    if cp.features != feature_digest() {
        return Err(Restart::FeatureTable);
    }
    if cp.lineage != head.lineage_id {
        return Err(Restart::Lineage);
    }
    let Some(b) = &cp.boundary else {
        return Ok(());
    };
    if b.seq == 0 || b.seq > head.durable_seq || b.offset < LINEAGE_LEN {
        return Err(Restart::BoundaryMissing);
    }
    if b.len > frame::MAX_PAYLOAD_LEN as u64 + frame::FIXED_HEADER_LEN as u64 + u16::MAX as u64
        || b.len < frame::FIXED_HEADER_LEN as u64
        || b.offset
            .checked_add(b.len)
            .is_none_or(|end| end > head.durable_end)
    {
        return Err(Restart::BoundaryLength);
    }
    let raw = bytes(source, b.offset, b.len).map_err(|_| Restart::BoundaryMissing)?;
    let h = frame::decode_header(&raw)
        .ok()
        .flatten()
        .ok_or(Restart::BoundaryHash)?;
    if h.frame_len() as u64 != b.len {
        return Err(Restart::BoundaryLength);
    }
    if Sha256::digest(&raw).as_slice() != b.hash {
        return Err(Restart::BoundaryHash);
    }
    if h.metadata.seq != b.seq {
        return Err(Restart::BoundarySequence);
    }
    Ok(())
}

pub(crate) struct RawFrame {
    pub boundary: Boundary,
    pub header: frame::Header,
    pub raw: Vec<u8>,
}

pub(crate) struct Corruption {
    pub offset: u64,
    pub seq: u64,
    pub reason: Restart,
    pub detail: String,
}

pub(crate) struct Scan {
    pub frames: Vec<RawFrame>,
    pub corruption: Option<Corruption>,
}

/// Header validation precedes allocation by payload length. The tail must be contiguous, even
/// when all of its records are irrelevant to billing.
pub(crate) fn scan(
    source: &(impl ReadAt + ?Sized),
    head: &HeadProof,
    boundary: Option<&Boundary>,
) -> io::Result<Scan> {
    let mut offset = boundary.map_or(LINEAGE_LEN, |b| b.offset + b.len);
    let mut seq = boundary.map_or(0, |b| b.seq);
    let mut frames = Vec::new();
    let mut corruption = None;
    while offset < head.durable_end {
        let result = read_frame(source, offset, head.durable_end);
        let (raw, header) = match result {
            Ok(Some(record)) => record,
            Ok(None) => {
                let valid = head.durable_end - offset >= LINEAGE_LEN
                    && lineage(&bytes(source, offset, LINEAGE_LEN)?) == Some(head.lineage_id);
                if valid {
                    offset += LINEAGE_LEN;
                    continue;
                }
                corruption = Some(Corruption {
                    offset,
                    seq: seq.saturating_add(1),
                    reason: Restart::InterveningLineage,
                    detail: "invalid interleaved lineage frame".into(),
                });
                break;
            }
            Err(ReadFrameError::Io(e)) => return Err(e),
            Err(ReadFrameError::Header(detail)) => {
                corruption = Some(Corruption {
                    offset,
                    seq: seq.saturating_add(1),
                    reason: Restart::TailBoundary,
                    detail,
                });
                break;
            }
        };
        if seq.checked_add(1) != Some(header.metadata.seq) {
            corruption = Some(Corruption {
                offset,
                seq: seq.saturating_add(1),
                reason: Restart::SequenceGap,
                detail: "noncontiguous WAL sequence".into(),
            });
            break;
        }
        seq = header.metadata.seq;
        let len = raw.len() as u64;
        frames.push(RawFrame {
            boundary: Boundary {
                seq,
                offset,
                len,
                hash: Sha256::digest(&raw).into(),
            },
            header,
            raw,
        });
        offset += len;
    }
    // A lineage frame is not a publication boundary. The last record must end at the proof.
    if corruption.is_none()
        && frames.last().is_some_and(|f| {
            f.boundary.offset + f.boundary.len != head.durable_end || !matches_head(&f.header, head)
        })
    {
        corruption = Some(Corruption {
            offset,
            seq: seq.saturating_add(1),
            reason: Restart::TailBoundary,
            detail: "published head does not match final record".into(),
        });
    }
    if corruption.is_none() && frames.is_empty() {
        let valid = if let Some(b) = boundary {
            let raw = bytes(source, b.offset, b.len)?;
            frame::decode_header(&raw)
                .ok()
                .flatten()
                .is_some_and(|h| b.offset + b.len == head.durable_end && matches_head(&h, head))
        } else {
            false
        };
        if !valid {
            corruption = Some(Corruption {
                offset,
                seq: seq.saturating_add(1),
                reason: Restart::TailBoundary,
                detail: "missing published final record".into(),
            });
        }
    }
    Ok(Scan { frames, corruption })
}

fn matches_head(header: &frame::Header, head: &HeadProof) -> bool {
    header.metadata.seq == head.durable_seq
        && header.identity == head.frame_identity
        && header.metadata.writer_id == head.writer_id
        && header.metadata.appended_ms == head.appended_ms
}

enum ReadFrameError {
    Io(io::Error),
    Header(String),
}
impl From<io::Error> for ReadFrameError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

fn read_frame(
    source: &(impl ReadAt + ?Sized),
    offset: u64,
    end: u64,
) -> Result<Option<(Vec<u8>, frame::Header)>, ReadFrameError> {
    if end - offset < 5 {
        return Err(ReadFrameError::Header("incomplete published header".into()));
    }
    let mut raw = bytes(source, offset, 5)?;
    if raw[4] == 2 {
        return Ok(None);
    }
    if raw[4] != frame::VERSION || end - offset < 47 {
        return Err(ReadFrameError::Header(
            "invalid published frame version or length".into(),
        ));
    }
    raw.extend(bytes(source, offset + 5, 42)?);
    let len = frame::FIXED_HEADER_LEN as u64 + u16::from_le_bytes([raw[45], raw[46]]) as u64;
    if len > end - offset {
        return Err(ReadFrameError::Header("incomplete published header".into()));
    }
    raw.extend(bytes(source, offset + 47, len - 47)?);
    let header = frame::decode_header(&raw)
        .map_err(|e: Error| ReadFrameError::Header(e.to_string()))?
        .ok_or_else(|| ReadFrameError::Header("incomplete published header".into()))?;
    if header.frame_len() as u64 > end - offset {
        return Err(ReadFrameError::Header(
            "incomplete published payload".into(),
        ));
    }
    raw.extend(bytes(source, offset + len, header.payload_len as u64)?);
    Ok(Some((raw, header)))
}
