//! Published heads are hints bound to a verified frame, never bare sequence watermarks.

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

/// A complete, contiguous prefix whose final frame matches the sidecar's identity.
/// No stored sequence or byte offset alone can construct this view.
pub struct PublishedLog<'a> {
    proof: HeadProof,
    frames: &'a [u8],
}

impl<'a> PublishedLog<'a> {
    pub fn open(log: &'a [u8], sidecar: &[u8]) -> Option<Self> {
        let proof = HeadProof::decode(sidecar)?;
        let end = usize::try_from(proof.durable_end).ok()?;
        let prefix = log.get(..end)?;
        if lineage(prefix)? != proof.lineage_id {
            return None;
        }
        let frames = prefix.get(LINEAGE_LEN..)?;
        let mut tail = frames;
        let mut seq = 0u64;
        let mut newest = None;
        let mut ended_on_record = false;
        while !tail.is_empty() {
            if tail.get(4) == Some(&2) {
                if lineage(tail)? != proof.lineage_id {
                    return None;
                }
                tail = tail.get(LINEAGE_LEN..)?;
                ended_on_record = false;
                continue;
            }
            let header = frame::decode_header(tail).ok()??;
            if header.metadata.seq != seq.checked_add(1)? || !frame::digest_verifies(tail) {
                return None;
            }
            tail = tail.get(header.frame_len()..)?;
            seq = header.metadata.seq;
            newest = Some(header);
            ended_on_record = true;
        }
        let header = newest?;
        if !ended_on_record
            || seq != proof.durable_seq
            || header.identity != proof.frame_identity
            || header.metadata.writer_id != proof.writer_id
            || header.metadata.appended_ms != proof.appended_ms
        {
            return None;
        }
        Some(Self { proof, frames })
    }

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
        let published = PublishedLog::open(&log, &proof.encode()).unwrap();
        let frames: Vec<_> = published.frames(&["visibility-late/v1"]).collect();
        assert_eq!(frames.len(), 1);
        assert!(
            matches!(&frames[0], Ok(frame::DecodeStep::Frame(f)) if f.header.metadata.seq == 1)
        );
        for length in 0..PROOF_LEN {
            assert!(PublishedLog::open(&log, &proof.encode()[..length]).is_none());
        }
        for at in 0..PROOF_LEN {
            let mut torn = proof.encode();
            torn[at] ^= 1;
            assert!(PublishedLog::open(&log, &torn).is_none());
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
            assert!(PublishedLog::open(&log, &changed.encode()).is_none());
        }
        let mut corrupt = log.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(PublishedLog::open(&corrupt, &proof.encode()).is_none());
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
        assert!(PublishedLog::open(&gapped, &changed.encode()).is_none());
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
            PublishedLog::open(&log, &past_record.encode()).is_none(),
            "proof end must name a record, not metadata"
        );
        log[between + 53] ^= 1;
        let mut hash = Sha256::new();
        hash.update(&log[between + 4..between + 21]);
        hash.update(&log[between + 53..between + LINEAGE_LEN]);
        log[between + 21..between + 53].copy_from_slice(&hash.finalize());
        assert!(
            PublishedLog::open(&log, &proof.encode()).is_none(),
            "even an intact conflicting lineage is refused"
        );
    }
}
