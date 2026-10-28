//! Published heads bind a trusted writer's durability claim to a bounded frame prefix.
//!
//! CRCs and frame identities detect corruption and bind content; they do not
//! authenticate fsync or defend against an actor forging both log and sidecar.
//! Trust the writer and filesystem. The raw codec and file APIs remain public
//! escape hatches: callers choose this view to enforce the published boundary.
//! A missing/torn proof is unavailable, not EOF. Billing progress additionally
//! requires fully consuming the bounded iterator, handling feature refusals and
//! persisting every PayloadCorrupt item as unreadable evidence before advancing.

use sha2::{Digest, Sha256};

use crate::{frame, Error};

const MAGIC: &[u8; 8] = b"BRHEAD01";
pub const PROOF_LEN: usize = 100;
const LINEAGE_LEN: usize = 69;

/// An unsynced sidecar written only after the covered log prefix has been synced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadProof {
    pub lineage_id: [u8; 16],
    pub writer_id: [u8; 16],
    pub durable_seq: u64,
    pub durable_end: u64,
    pub frame_identity: [u8; 32],
    pub appended_ms: u64,
}

impl HeadProof {
    /// Fixed-width little-endian fields followed by CRC32C over all preceding bytes.
    pub fn encode(&self) -> [u8; PROOF_LEN] {
        let mut out = [0; PROOF_LEN];
        out[..8].copy_from_slice(MAGIC);
        out[8..24].copy_from_slice(&self.lineage_id);
        out[24..40].copy_from_slice(&self.writer_id);
        out[40..48].copy_from_slice(&self.durable_seq.to_le_bytes());
        out[48..56].copy_from_slice(&self.durable_end.to_le_bytes());
        out[56..88].copy_from_slice(&self.frame_identity);
        out[88..96].copy_from_slice(&self.appended_ms.to_le_bytes());
        let crc = crc32c(&out[..96]);
        out[96..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// A torn or missing proof is unpublished, not permission to read to EOF.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != PROOF_LEN
            || &bytes[..8] != MAGIC
            || crc32c(&bytes[..96]) != u32::from_le_bytes(bytes[96..].try_into().ok()?)
        {
            return None;
        }
        Some(Self {
            lineage_id: bytes[8..24].try_into().ok()?,
            writer_id: bytes[24..40].try_into().ok()?,
            durable_seq: u64::from_le_bytes(bytes[40..48].try_into().ok()?),
            durable_end: u64::from_le_bytes(bytes[48..56].try_into().ok()?),
            frame_identity: bytes[56..88].try_into().ok()?,
            appended_ms: u64::from_le_bytes(bytes[88..96].try_into().ok()?),
        })
    }
}

/// Why a CRC-valid durability claim cannot describe the supplied prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProofError {
    /// A proof belongs to another lineage or final frame, including an orphan at relocation.
    MismatchedProof,
    /// A covered header cannot be trusted, so its bytes cannot safely be skipped.
    Header { offset: usize, source: Error },
    /// A covered frame is missing, incomplete, noncontiguous or has invalid lineage metadata.
    CorruptPrefix { offset: usize, reason: &'static str },
}

impl std::fmt::Display for ProofError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MismatchedProof => write!(
                f,
                "durable-head proof does not match this log's lineage or final frame"
            ),
            Self::Header { offset, source } => {
                write!(f, "covered WAL header at byte {offset}: {source}")
            }
            Self::CorruptPrefix { offset, reason } => {
                write!(f, "covered WAL prefix at byte {offset}: {reason}")
            }
        }
    }
}
impl std::error::Error for ProofError {}

/// A contiguous prefix whose final verified header matches the sidecar's identity.
/// Payload integrity is reported by iteration so damaged bytes can be retained as
/// unreadable evidence. No bare stored sequence or offset can construct this view.
pub struct PublishedLog<'a> {
    proof: HeadProof,
    frames: &'a [u8],
}

impl<'a> PublishedLog<'a> {
    /// `Ok(None)` means the proof is absent, torn or fails its own checksum.
    /// Errors instead identify a mismatched claim or corruption in its covered
    /// prefix. Payload corruption is yielded as evidence by `frames`, not hidden.
    pub fn open(log: &'a [u8], sidecar: &[u8]) -> Result<Option<Self>, ProofError> {
        let Some(proof) = HeadProof::decode(sidecar) else {
            return Ok(None);
        };
        let end = usize::try_from(proof.durable_end).map_err(|_| ProofError::CorruptPrefix {
            offset: log.len(),
            reason: "published end cannot be represented",
        })?;
        let prefix = log.get(..end).ok_or(ProofError::CorruptPrefix {
            offset: log.len(),
            reason: "published prefix is truncated",
        })?;
        let first_lineage = lineage(prefix).ok_or(ProofError::CorruptPrefix {
            offset: 0,
            reason: "missing or corrupt lineage metadata",
        })?;
        if first_lineage != proof.lineage_id {
            return Err(ProofError::MismatchedProof);
        }
        let frames = prefix.get(LINEAGE_LEN..).ok_or(ProofError::CorruptPrefix {
            offset: 0,
            reason: "incomplete lineage metadata",
        })?;
        let mut tail = frames;
        let mut seq = 0u64;
        let mut newest = None;
        let mut ended_on_record = false;
        while !tail.is_empty() {
            let offset = end - tail.len();
            if tail.get(4) == Some(&2) {
                if lineage(tail) != Some(proof.lineage_id) {
                    return Err(ProofError::CorruptPrefix {
                        offset,
                        reason: "corrupt or conflicting lineage metadata",
                    });
                }
                tail = &tail[LINEAGE_LEN..];
                ended_on_record = false;
                continue;
            }
            let header = frame::decode_header(tail)
                .map_err(|source| ProofError::Header { offset, source })?
                .ok_or(ProofError::CorruptPrefix {
                    offset,
                    reason: "incomplete published header",
                })?;
            if Some(header.metadata.seq) != seq.checked_add(1) {
                return Err(ProofError::CorruptPrefix {
                    offset,
                    reason: "noncontiguous record sequence",
                });
            }
            tail = tail
                .get(header.frame_len()..)
                .ok_or(ProofError::CorruptPrefix {
                    offset,
                    reason: "incomplete published frame",
                })?;
            seq = header.metadata.seq;
            newest = Some(header);
            ended_on_record = true;
        }
        let header = newest.ok_or(ProofError::MismatchedProof)?;
        if !ended_on_record
            || seq != proof.durable_seq
            || header.identity != proof.frame_identity
            || header.metadata.writer_id != proof.writer_id
            || header.metadata.appended_ms != proof.appended_ms
        {
            return Err(ProofError::MismatchedProof);
        }
        Ok(Some(Self { proof, frames }))
    }

    /// The bound, not a billing-consumption receipt. Feature compatibility and
    /// payload integrity are established only while fully consuming `frames`.
    pub fn head(&self) -> &HeadProof {
        &self.proof
    }

    /// Unknown features retain the frame codec's typed refusal/skip policy. Even
    /// a complete, readable frame past the proof is never offered to that codec.
    pub fn frames<'k>(&self, known: &'k [&'k str]) -> PublishedFrames<'a, 'k> {
        PublishedFrames {
            tail: self.frames,
            known,
        }
    }
}

#[must_use = "consume the complete bounded iterator before advancing billing progress"]
pub struct PublishedFrames<'a, 'k> {
    tail: &'a [u8],
    known: &'k [&'k str],
}

impl<'a> Iterator for PublishedFrames<'a, '_> {
    type Item = Result<frame::DecodeStep<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // PublishedLog validated these interleaved lineage frames already. They
        // carry no record sequence and must not be yielded as content frames.
        while self.tail.get(4) == Some(&2) {
            self.tail = &self.tail[LINEAGE_LEN..];
        }
        if self.tail.is_empty() {
            return None;
        }
        let step = frame::decode_next(self.tail, self.known);
        match &step {
            Ok(frame::DecodeStep::Frame(frame)) => self.tail = &self.tail[frame.consumed..],
            Ok(frame::DecodeStep::Skipped { consumed, .. })
            | Ok(frame::DecodeStep::PayloadCorrupt { consumed, .. }) => {
                self.tail = &self.tail[*consumed..]
            }
            Ok(frame::DecodeStep::Incomplete) | Err(_) => self.tail = &[],
        }
        Some(step)
    }
}

fn lineage(bytes: &[u8]) -> Option<[u8; 16]> {
    let bytes = bytes.get(..LINEAGE_LEN)?;
    if bytes[..4] != 16u32.to_le_bytes() || bytes[4] != 2 || bytes[5..21] != [0; 16] {
        return None;
    }
    let mut hash = Sha256::new();
    hash.update(&bytes[4..21]);
    hash.update(&bytes[53..]);
    if hash.finalize().as_slice() != &bytes[21..53] {
        return None;
    }
    bytes[53..].try_into().ok()
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (Vec<u8>, HeadProof) {
        let id = [7; 16];
        let mut log = Vec::new();
        log.extend_from_slice(&16u32.to_le_bytes());
        log.push(2);
        log.extend_from_slice(&[0; 16]);
        let mut hash = Sha256::new();
        hash.update(&log[4..]);
        hash.update(id);
        log.extend_from_slice(&hash.finalize());
        log.extend_from_slice(&id);
        let frame = frame::encode(
            frame::Metadata {
                seq: 1,
                fence: 9,
                appended_ms: 1000,
                writer_id: [3; 16],
            },
            &["visibility-late/v1"],
            b"{}",
        )
        .unwrap();
        let header = frame::decode_header(&frame).unwrap().unwrap();
        log.extend_from_slice(&frame);
        let proof = HeadProof {
            lineage_id: id,
            writer_id: [3; 16],
            durable_seq: 1,
            durable_end: log.len() as u64,
            frame_identity: header.identity,
            appended_ms: 1000,
        };
        (log, proof)
    }

    #[test]
    fn crate_never_yields_past_proof() {
        let (mut log, proof) = fixture();
        log.extend_from_slice(
            &frame::encode(
                frame::Metadata {
                    seq: 2,
                    fence: 9,
                    appended_ms: 2000,
                    writer_id: [3; 16],
                },
                &[],
                b"{\"unpublished\":true}",
            )
            .unwrap(),
        );
        let published = PublishedLog::open(&log, &proof.encode()).unwrap().unwrap();
        let frames: Vec<_> = published.frames(&["visibility-late/v1"]).collect();
        assert_eq!(frames.len(), 1);
        assert!(
            matches!(&frames[0], Ok(frame::DecodeStep::Frame(f)) if f.header.metadata.seq == 1)
        );
        for length in 0..PROOF_LEN {
            assert!(PublishedLog::open(&log, &proof.encode()[..length])
                .unwrap()
                .is_none());
        }
        for at in 0..PROOF_LEN {
            let mut torn = proof.encode();
            torn[at] ^= 1;
            assert!(PublishedLog::open(&log, &torn).unwrap().is_none());
        }
    }

    #[test]
    fn proof_requires_matching_frame_and_contiguous_prefix() {
        let (log, proof) = fixture();
        for field in 0..6 {
            let mut changed = proof.clone();
            match field {
                0 => changed.lineage_id[0] ^= 1,
                1 => changed.writer_id[0] ^= 1,
                2 => changed.durable_seq += 1,
                3 => changed.durable_end -= 1,
                4 => changed.frame_identity[0] ^= 1,
                5 => changed.appended_ms += 1,
                _ => unreachable!(),
            }
            assert!(PublishedLog::open(&log, &changed.encode()).is_err());
        }
        let mut corrupt = log.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        let bounded = PublishedLog::open(&corrupt, &proof.encode())
            .unwrap()
            .unwrap();
        assert!(matches!(
            bounded.frames(&["visibility-late/v1"]).next(),
            Some(Ok(frame::DecodeStep::PayloadCorrupt { .. }))
        ));
        let mut gapped = log;
        let frame = frame::encode(
            frame::Metadata {
                seq: 3,
                fence: 9,
                appended_ms: 3000,
                writer_id: [3; 16],
            },
            &[],
            b"{}",
        )
        .unwrap();
        let header = frame::decode_header(&frame).unwrap().unwrap();
        gapped.extend_from_slice(&frame);
        let changed = HeadProof {
            durable_seq: 3,
            durable_end: gapped.len() as u64,
            frame_identity: header.identity,
            appended_ms: 3000,
            ..proof
        };
        assert!(PublishedLog::open(&gapped, &changed.encode()).is_err());
    }

    #[test]
    fn interleaved_lineage_is_validated_but_never_yielded() {
        let (mut log, proof) = fixture();
        let lineage = log[..LINEAGE_LEN].to_vec();
        let between = log.len();
        log.extend_from_slice(&lineage);
        let second = frame::encode(
            frame::Metadata {
                seq: 2,
                fence: 9,
                appended_ms: 2000,
                writer_id: [3; 16],
            },
            &[],
            b"{}",
        )
        .unwrap();
        let header = frame::decode_header(&second).unwrap().unwrap();
        log.extend_from_slice(&second);
        let proof = HeadProof {
            durable_seq: 2,
            durable_end: log.len() as u64,
            frame_identity: header.identity,
            appended_ms: 2000,
            ..proof
        };
        assert_eq!(
            PublishedLog::open(&log, &proof.encode())
                .unwrap()
                .unwrap()
                .frames(&["visibility-late/v1"])
                .count(),
            2
        );
        log.extend_from_slice(&lineage);
        let past_record = HeadProof {
            durable_end: log.len() as u64,
            ..proof.clone()
        };
        assert!(
            PublishedLog::open(&log, &past_record.encode()).is_err(),
            "proof end must name a record, not metadata"
        );
        log[between + 53] ^= 1;
        let mut hash = Sha256::new();
        hash.update(&log[between + 4..between + 21]);
        hash.update(&log[between + 53..between + LINEAGE_LEN]);
        log[between + 21..between + 53].copy_from_slice(&hash.finalize());
        assert!(
            PublishedLog::open(&log, &proof.encode()).is_err(),
            "even an intact conflicting lineage is refused"
        );
    }

    #[test]
    fn unavailable_proof_is_distinct_from_covered_header_corruption() {
        let (mut log, proof) = fixture();
        assert!(PublishedLog::open(&log, &[]).unwrap().is_none());
        log[LINEAGE_LEN + 5] ^= 1;
        assert!(matches!(
            PublishedLog::open(&log, &proof.encode()),
            Err(ProofError::Header {
                offset: LINEAGE_LEN,
                ..
            })
        ));
    }

    #[test]
    fn unreadable_payload_evidence_is_bounded_and_iteration_continues() {
        let (mut log, proof) = fixture();
        let damaged_start = LINEAGE_LEN;
        *log.last_mut().unwrap() ^= 1;
        let damaged = log[damaged_start..].to_vec();
        let second = frame::encode(
            frame::Metadata {
                seq: 2,
                fence: 9,
                appended_ms: 2000,
                writer_id: [3; 16],
            },
            &[],
            b"{}",
        )
        .unwrap();
        let header = frame::decode_header(&second).unwrap().unwrap();
        log.extend_from_slice(&second);
        let proof = HeadProof {
            durable_seq: 2,
            durable_end: log.len() as u64,
            frame_identity: header.identity,
            appended_ms: 2000,
            ..proof
        };
        log.extend_from_slice(&second);
        let bounded = PublishedLog::open(&log, &proof.encode()).unwrap().unwrap();
        let mut frames = bounded.frames(&["visibility-late/v1"]);
        assert!(
            matches!(frames.next(), Some(Ok(frame::DecodeStep::PayloadCorrupt { raw_frame, header, .. })) if raw_frame == damaged && header.metadata.seq == 1)
        );
        assert!(
            matches!(frames.next(), Some(Ok(frame::DecodeStep::Frame(f))) if f.header.metadata.seq == 2)
        );
        assert!(
            frames.next().is_none(),
            "unpublished duplicate bytes remain outside the iterator"
        );
    }

    #[test]
    fn a_head_is_not_a_billing_consumption_receipt() {
        let (mut log, proof) = fixture();
        let second = frame::encode(
            frame::Metadata {
                seq: 2,
                fence: 9,
                appended_ms: 2000,
                writer_id: [3; 16],
            },
            &["bill-new/v1"],
            b"{}",
        )
        .unwrap();
        let header = frame::decode_header(&second).unwrap().unwrap();
        log.extend_from_slice(&second);
        let proof = HeadProof {
            durable_seq: 2,
            durable_end: log.len() as u64,
            frame_identity: header.identity,
            appended_ms: 2000,
            ..proof
        };
        let bounded = PublishedLog::open(&log, &proof.encode()).unwrap().unwrap();
        assert_eq!(bounded.head().durable_seq, 2);
        let mut frames = bounded.frames(&["visibility-late/v1"]);
        assert!(frames.next().unwrap().is_ok());
        assert!(
            matches!(frames.next(), Some(Err(Error::NewerWriter { features })) if features == ["bill-new/v1"])
        );
        assert!(frames.next().is_none());
    }
}
