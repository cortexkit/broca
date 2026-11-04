//! Frames: how one record sits in an open log, encrypted.
//!
//! ```text
//! length      u32 LE: bytes after this field (header, checksum, nonce, ciphertext, tag)
//! header      35 bytes, fixed layout below; plaintext but authenticated
//! header_crc  u32 LE: CRC32C over the length and the header
//! nonce       24 random bytes
//! ciphertext  the record body, XChaCha20-Poly1305
//! tag         16 bytes
//! ```
//!
//! The header has a fixed width so a reader that has met damage can test any byte offset for a
//! frame start: a header that passes its checksum, then a frame that authenticates.
//!
//! Classification of a frame that fails to read, which recovery depends on:
//! - fewer bytes than a header, or fewer than the length says: **incomplete**, possibly a write
//!   that never finished;
//! - a complete header failing its checksum: **damage**, never an unfinished write, so a damaged
//!   length can't make acknowledged bytes look like a torn tail;
//! - a frame failing authentication: **damage**.
//!
//! The authenticated data binds each frame to its place: the caller's `bind` bytes (the session
//! id and the log's name), then the length and the header. A frame moved to another log, another
//! session, or another position fails to authenticate or fails the frame-index check.
//!
//! Nonces are random, never derived from position: a crash can leave a log's tail rewritten at
//! the same position with different bytes, and a repeated nonce under a different plaintext would
//! break the cipher. XChaCha20's 192-bit nonce makes random nonces safe at any volume under one key.

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{Key, Tag, XChaCha20Poly1305, XNonce};

pub const FRAME_VERSION: u8 = 1;
pub const HEADER_LEN: usize = 35;
const LENGTH_LEN: usize = 4;
const CRC_LEN: usize = 4;
const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;
/// Bytes a frame adds around a record body.
pub const FRAME_OVERHEAD: usize = LENGTH_LEN + HEADER_LEN + CRC_LEN + NONCE_LEN + TAG_LEN;
/// The largest body a frame may carry. Larger bodies are blobs; a length above this is damage.
pub const MAX_BODY: usize = 64 << 20;

/// The record can be skipped by a reader that doesn't know its kind.
pub const FLAG_SKIPPABLE: u8 = 1;
/// A claim-only frame (5.2): no record, only a sync claim.
pub const FLAG_CLAIM_ONLY: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameStream {
    /// Checkpoint and claim-only frames: not records of either stream.
    None,
    Transcript,
    Ledger,
}

/// A frame's plaintext header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub flags: u8,
    pub stream: FrameStream,
    pub kind: u8,
    pub kind_version: u8,
    pub key_id: u16,
    /// 0 for frames that aren't records.
    pub seq: u64,
    /// The first seq of the batch this frame was appended in.
    pub batch_first: u64,
    /// How many frames that batch holds.
    pub batch_count: u16,
    /// This frame's position in its log, from 0, with no gaps.
    pub frame_index: u32,
    /// The log offset the writer's last completed barrier covered when this frame was written.
    pub synced_through: u64,
    /// The seq of the previous record in this frame's stream, if any.
    pub prev_in_stream: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    HeaderChecksum,
    Authentication,
    UnknownVersion(u8),
    UnknownKey(u16),
    /// A header field out of range: a batch or back-link pointing past the frame's own seq,
    /// a length larger than any frame. Damage, like a checksum failure.
    BadHeader(&'static str),
    BodyTooLarge,
}

/// A data key with the id frames name it by.
pub struct FrameKey {
    id: u16,
    cipher: XChaCha20Poly1305,
}

impl FrameKey {
    pub fn new(id: u16, key: &[u8; 32]) -> Self {
        Self {
            id,
            cipher: XChaCha20Poly1305::new(Key::from_slice(key)),
        }
    }

    pub fn id(&self) -> u16 {
        self.id
    }
}

/// The result of reading a frame from the start of a byte slice.
#[derive(Debug, PartialEq, Eq)]
pub enum FrameRead {
    Frame {
        header: FrameHeader,
        body: Vec<u8>,
        /// Bytes the frame occupies.
        len: usize,
    },
    /// The slice ends before the frame does.
    Incomplete,
}

fn write_header(header: &FrameHeader, out: &mut Vec<u8>) -> Result<(), FrameError> {
    let stream = match header.stream {
        FrameStream::None => 0u8,
        FrameStream::Transcript => 1,
        FrameStream::Ledger => 2,
    };
    let batch_back = header
        .seq
        .checked_sub(header.batch_first)
        .and_then(|d| u16::try_from(d).ok())
        .ok_or(FrameError::BadHeader(
            "batch start after the frame or too far before it",
        ))?;
    let prev_back = match header.prev_in_stream {
        None => 0u32,
        Some(prev) => header
            .seq
            .checked_sub(prev)
            .filter(|d| *d > 0)
            .and_then(|d| u32::try_from(d).ok())
            .ok_or(FrameError::BadHeader("back-link not before the frame"))?,
    };
    out.push(FRAME_VERSION);
    out.push(header.flags);
    out.push(stream);
    out.push(header.kind);
    out.push(header.kind_version);
    out.extend_from_slice(&header.key_id.to_le_bytes());
    out.extend_from_slice(&header.seq.to_le_bytes());
    out.extend_from_slice(&batch_back.to_le_bytes());
    out.extend_from_slice(&header.batch_count.to_le_bytes());
    out.extend_from_slice(&header.frame_index.to_le_bytes());
    out.extend_from_slice(&header.synced_through.to_le_bytes());
    out.extend_from_slice(&prev_back.to_le_bytes());
    Ok(())
}

fn read_header(bytes: &[u8; HEADER_LEN]) -> Result<FrameHeader, FrameError> {
    let u16_at = |i: usize| u16::from_le_bytes([bytes[i], bytes[i + 1]]);
    let u32_at = |i: usize| u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
    let u64_at = |i: usize| u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
    if bytes[0] != FRAME_VERSION {
        return Err(FrameError::UnknownVersion(bytes[0]));
    }
    let stream = match bytes[2] {
        0 => FrameStream::None,
        1 => FrameStream::Transcript,
        2 => FrameStream::Ledger,
        _ => return Err(FrameError::BadHeader("unknown stream")),
    };
    let seq = u64_at(7);
    let batch_first = seq
        .checked_sub(u64::from(u16_at(15)))
        .ok_or(FrameError::BadHeader("batch starts before seq 0"))?;
    let prev_back = u32_at(31);
    let prev_in_stream = match prev_back {
        0 => None,
        back => Some(
            seq.checked_sub(u64::from(back))
                .ok_or(FrameError::BadHeader("back-link before seq 0"))?,
        ),
    };
    Ok(FrameHeader {
        flags: bytes[1],
        stream,
        kind: bytes[3],
        kind_version: bytes[4],
        key_id: u16_at(5),
        seq,
        batch_first,
        batch_count: u16_at(17),
        frame_index: u32_at(19),
        synced_through: u64_at(23),
        prev_in_stream,
    })
}

/// Encrypts `body` into a frame. `bind` names where the frame lives (session id and log name);
/// a reader must pass the same bytes.
pub fn seal(
    header: &FrameHeader,
    body: &[u8],
    key: &FrameKey,
    bind: &[u8],
) -> Result<Vec<u8>, FrameError> {
    if body.len() > MAX_BODY {
        return Err(FrameError::BodyTooLarge);
    }
    if header.key_id != key.id {
        return Err(FrameError::UnknownKey(header.key_id));
    }
    let length = (HEADER_LEN + CRC_LEN + NONCE_LEN + body.len() + TAG_LEN) as u32;
    let mut out = Vec::with_capacity(LENGTH_LEN + length as usize);
    out.extend_from_slice(&length.to_le_bytes());
    write_header(header, &mut out)?;
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());

    let mut nonce = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce).expect("the OS random source is available");
    out.extend_from_slice(&nonce);

    let mut aad = Vec::with_capacity(bind.len() + LENGTH_LEN + HEADER_LEN);
    aad.extend_from_slice(bind);
    aad.extend_from_slice(&out[..LENGTH_LEN + HEADER_LEN]);
    let mut buffer = body.to_vec();
    let tag = key
        .cipher
        .encrypt_in_place_detached(XNonce::from_slice(&nonce), &aad, &mut buffer)
        .expect("encryption of a bounded body cannot fail");
    out.extend_from_slice(&buffer);
    out.extend_from_slice(&tag);
    Ok(out)
}

/// Reads and decrypts the frame at the start of `bytes`. `key` finds a data key by its id.
pub fn open<'k>(
    bytes: &[u8],
    key: impl Fn(u16) -> Option<&'k FrameKey>,
    bind: &[u8],
) -> Result<FrameRead, FrameError> {
    let fixed = LENGTH_LEN + HEADER_LEN + CRC_LEN;
    if bytes.len() < fixed {
        return Ok(FrameRead::Incomplete);
    }
    let length = u32::from_le_bytes(bytes[..LENGTH_LEN].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[LENGTH_LEN + HEADER_LEN..fixed].try_into().unwrap());
    if crc32c::crc32c(&bytes[..LENGTH_LEN + HEADER_LEN]) != crc {
        return Err(FrameError::HeaderChecksum);
    }
    let header_bytes: &[u8; HEADER_LEN] = bytes[LENGTH_LEN..LENGTH_LEN + HEADER_LEN]
        .try_into()
        .unwrap();
    let header = read_header(header_bytes)?;
    let smallest = HEADER_LEN + CRC_LEN + NONCE_LEN + TAG_LEN;
    if !(smallest..=smallest + MAX_BODY).contains(&length) {
        return Err(FrameError::BadHeader("length out of range"));
    }
    let total = LENGTH_LEN + length;
    if bytes.len() < total {
        return Ok(FrameRead::Incomplete);
    }
    let cipher = key(header.key_id).ok_or(FrameError::UnknownKey(header.key_id))?;
    let nonce = &bytes[fixed..fixed + NONCE_LEN];
    let ciphertext = &bytes[fixed + NONCE_LEN..total - TAG_LEN];
    let tag = &bytes[total - TAG_LEN..total];

    let mut aad = Vec::with_capacity(bind.len() + LENGTH_LEN + HEADER_LEN);
    aad.extend_from_slice(bind);
    aad.extend_from_slice(&bytes[..LENGTH_LEN + HEADER_LEN]);
    let mut body = ciphertext.to_vec();
    cipher
        .cipher
        .decrypt_in_place_detached(
            XNonce::from_slice(nonce),
            &aad,
            &mut body,
            Tag::from_slice(tag),
        )
        .map_err(|_| FrameError::Authentication)?;
    Ok(FrameRead::Frame {
        header,
        body,
        len: total,
    })
}

#[cfg(test)]
mod tests;
