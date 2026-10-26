//! Version 4 wire layout (integers are little-endian):
//!
//! ```text
//! len:u32 | ver:u8=4 | seq:u64 | fence:u64 | appended_ms:u64 | writer_id:16
//! requires_len:u16 | requires:UTF-8 JSON array | payload_sha256:32
//! header_crc32c:u32 | payload:len
//! ```
//!
//! CRC32C covers the header before the CRC field. Identity is SHA-256 over the
//! entire header, including the CRC. The payload is not a version-3 envelope.

use sha2::{Digest, Sha256};

use crate::features::{self, Policy};
use crate::{Error, HeaderCorruption};

pub const VERSION: u8 = 4;
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;
/// Header size excluding the variable-length requires array.
pub const FIXED_HEADER_LEN: usize = 83;
const REQUIRES_START: usize = 47;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub seq: u64,
    pub fence: u64,
    pub appended_ms: u64,
    pub writer_id: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub metadata: Metadata,
    pub requires: Vec<String>,
    pub payload_len: u32,
    pub payload_sha256: [u8; 32],
    pub header_len: usize,
    pub identity: [u8; 32],
}

impl Header {
    pub fn frame_len(&self) -> usize {
        self.header_len + self.payload_len as usize
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame<'a> {
    pub header: Header,
    pub payload: &'a [u8],
    pub consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeStep<'a> {
    Frame(Frame<'a>),
    /// A complete frame requiring only unknown content features.
    Skipped {
        header: Header,
        consumed: usize,
    },
    /// Header verified, payload digest failed. Preserve these bytes as evidence
    /// before advancing by `consumed` to the next frame.
    PayloadCorrupt {
        header: Header,
        raw_frame: &'a [u8],
        consumed: usize,
    },
    Incomplete,
}

/// Encode only the header, using a payload digest computed by the caller. This
/// keeps payload serialization and hashing off a writer's stamping thread.
pub fn encode_header(
    metadata: Metadata,
    requires: &[&str],
    payload_len: usize,
    payload_sha256: [u8; 32],
) -> Result<Vec<u8>, Error> {
    if payload_len > MAX_PAYLOAD_LEN as usize {
        return Err(Error::PayloadTooLarge { len: payload_len });
    }
    for name in requires {
        if !features::is_feature_name(name) {
            return Err(Error::InvalidFeatureName {
                name: (*name).to_owned(),
            });
        }
    }
    let mut names = requires.to_vec();
    names.sort_unstable();
    names.dedup();
    // Strings are already validated ASCII. Serializing a string array cannot fail.
    let requires = serde_json::to_vec(&names).expect("feature string array serializes");
    let requires_len = u16::try_from(requires.len()).map_err(|_| Error::RequiresTooLarge {
        len: requires.len(),
    })?;
    let mut out = Vec::with_capacity(FIXED_HEADER_LEN + requires.len());
    out.extend_from_slice(&(payload_len as u32).to_le_bytes());
    out.push(VERSION);
    out.extend_from_slice(&metadata.seq.to_le_bytes());
    out.extend_from_slice(&metadata.fence.to_le_bytes());
    out.extend_from_slice(&metadata.appended_ms.to_le_bytes());
    out.extend_from_slice(&metadata.writer_id);
    out.extend_from_slice(&requires_len.to_le_bytes());
    out.extend_from_slice(&requires);
    out.extend_from_slice(&payload_sha256);
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

pub fn encode(metadata: Metadata, requires: &[&str], payload: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = encode_header(
        metadata,
        requires,
        payload.len(),
        Sha256::digest(payload).into(),
    )?;
    out.extend_from_slice(payload);
    Ok(out)
}

/// Verify a header without needing the payload. `None` means more bytes are
/// needed, not permission to discard a tail before the log's published end.
pub fn decode_header(buf: &[u8]) -> Result<Option<Header>, Error> {
    if buf.len() < 5 {
        return Ok(None);
    }
    if buf[4] != VERSION {
        return Err(Error::UnsupportedVersion { ver: buf[4] });
    }
    if buf.len() < REQUIRES_START {
        return Ok(None);
    }
    let requires_len = u16::from_le_bytes(buf[45..47].try_into().unwrap()) as usize;
    let header_len = FIXED_HEADER_LEN + requires_len;
    if buf.len() < header_len {
        return Ok(None);
    }
    let crc_start = header_len - 4;
    let stored_crc = u32::from_le_bytes(buf[crc_start..header_len].try_into().unwrap());
    if crc32c(&buf[..crc_start]) != stored_crc {
        return Err(corrupt(HeaderCorruption::CrcMismatch));
    }
    let payload_len = u32::from_le_bytes(buf[..4].try_into().unwrap());
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(corrupt(HeaderCorruption::PayloadTooLarge {
            len: payload_len,
        }));
    }
    let requires: Vec<String> =
        serde_json::from_slice(&buf[REQUIRES_START..REQUIRES_START + requires_len])
            .map_err(|_| corrupt(HeaderCorruption::InvalidRequires))?;
    for name in &requires {
        if !features::is_feature_name(name) {
            return Err(corrupt(HeaderCorruption::InvalidFeatureName {
                name: name.clone(),
            }));
        }
    }
    Ok(Some(Header {
        metadata: Metadata {
            seq: u64::from_le_bytes(buf[5..13].try_into().unwrap()),
            fence: u64::from_le_bytes(buf[13..21].try_into().unwrap()),
            appended_ms: u64::from_le_bytes(buf[21..29].try_into().unwrap()),
            writer_id: buf[29..45].try_into().unwrap(),
        },
        requires,
        payload_len,
        payload_sha256: buf[crc_start - 32..crc_start].try_into().unwrap(),
        header_len,
        identity: Sha256::digest(&buf[..header_len]).into(),
    }))
}

/// Decode for the billing reader. Unknown `bill-` features refuse the log;
/// unknown content features skip the frame without interpreting its payload.
pub fn decode_next<'a>(buf: &'a [u8], known: &[&str]) -> Result<DecodeStep<'a>, Error> {
    decode_next_with_policy(buf, known, Policy::Billing)
}

/// Broca uses `Policy::All` to retain its stricter must-understand rule.
pub fn decode_next_with_policy<'a>(
    buf: &'a [u8],
    known: &[&str],
    policy: Policy,
) -> Result<DecodeStep<'a>, Error> {
    let Some(header) = decode_header(buf)? else {
        return Ok(DecodeStep::Incomplete);
    };
    // Requirements live in the verified header. Refuse an incompatible log even
    // when its payload is damaged or has not arrived yet.
    let skip = features::must_skip(&header.requires, known, policy)?;
    let consumed = header.frame_len();
    if buf.len() < consumed {
        return Ok(DecodeStep::Incomplete);
    }
    let payload = &buf[header.header_len..consumed];
    if Sha256::digest(payload).as_slice() != header.payload_sha256 {
        return Ok(DecodeStep::PayloadCorrupt {
            header,
            raw_frame: &buf[..consumed],
            consumed,
        });
    }
    if skip {
        return Ok(DecodeStep::Skipped { header, consumed });
    }
    Ok(DecodeStep::Frame(Frame {
        header,
        payload,
        consumed,
    }))
}

/// Check integrity without applying a reader's feature table.
pub fn digest_verifies(buf: &[u8]) -> bool {
    let Ok(Some(header)) = decode_header(buf) else {
        return false;
    };
    buf.get(header.header_len..header.frame_len())
        .is_some_and(|payload| Sha256::digest(payload).as_slice() == header.payload_sha256)
}

fn corrupt(reason: HeaderCorruption) -> Error {
    Error::HeaderCorrupt { reason }
}

// Reflected Castagnoli polynomial, initial/final XOR all ones. Headers are
// bounded by a u16 requires length, so a portable implementation stays small.
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
    #[test]
    fn castagnoli_check_value() {
        assert_eq!(super::crc32c(b"123456789"), 0xe306_9283);
    }
}
