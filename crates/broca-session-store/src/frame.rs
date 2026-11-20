//! Frames: how one record sits in an open log, sealed.
//!
//! ```text
//! length      u32 LE: bytes after this field (header, checksum, then the sealed body)
//! header      35 bytes, fixed layout below; plaintext but authenticated
//! header_crc  u32 LE: CRC32C over the length and the header
//! body        the record body as one sealed unit (see `FrameKey::seal_unit`):
//!               encrypted: 24 random nonce bytes, the XChaCha20-Poly1305 ciphertext, a 16-byte tag
//!               plain: the record body, then a 16-byte unkeyed check
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
//! - a frame failing authentication, or its plain check: **damage**.
//!
//! The authenticated data binds each frame to its place: the caller's `bind` bytes (the session
//! id and the log's name), then the length and the header. A frame moved to another log, another
//! session, or another position fails to authenticate or fails the frame-index check. A plain
//! frame's check covers the same bytes, so the same holds for it.
//!
//! Nonces are random, never derived from position: a crash can leave a log's tail rewritten at
//! the same position with different bytes, and a repeated nonce under a different plaintext would
//! break the cipher. XChaCha20's 192-bit nonce makes random nonces safe at any volume under one key.

use sha2::{Digest as _, Sha256};

use crate::keys::Cipher;

pub const FRAME_VERSION: u8 = 1;
pub const HEADER_LEN: usize = 35;
const LENGTH_LEN: usize = 4;
const CRC_LEN: usize = 4;
const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;
/// Bytes a plain unit's check adds after its plaintext.
pub const CHECK_LEN: usize = 16;
/// What a plain unit's check is the SHA-256 of, before its authenticated data and plaintext.
const CHECK_CONTEXT: &[u8] = b"broca-plain-unit-v1";
/// Bytes an encrypted frame adds around a record body. A plain frame adds
/// [`PLAIN_FRAME_OVERHEAD`]; [`FrameKey::frame_overhead`] gives a key's.
pub const FRAME_OVERHEAD: usize = LENGTH_LEN + HEADER_LEN + CRC_LEN + NONCE_LEN + TAG_LEN;
/// Bytes a plain frame adds around a record body.
pub const PLAIN_FRAME_OVERHEAD: usize = LENGTH_LEN + HEADER_LEN + CRC_LEN + CHECK_LEN;
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

/// A session's key with the id frames name it by, in the session's mode: a data key's cipher for
/// an encrypted session, or none for a plain one. Every sealed unit of the session goes through
/// [`seal_unit`](Self::seal_unit) and [`open_unit`](Self::open_unit), so the mode lives here and
/// callers don't change with it.
#[derive(Clone)]
pub struct FrameKey {
    id: u16,
    /// `None` for a plain key.
    cipher: Option<XChaCha>,
}

/// XChaCha20-Poly1305 (draft-irtf-cfrg-xchacha) built from its two halves: HChaCha20 derives a
/// subkey from the key and the nonce's first 16 bytes, and ChaCha20-Poly1305 (RFC 8439) seals
/// under that subkey with a 12-byte nonce of four zero bytes and the nonce's last 8. ring's
/// ChaCha20-Poly1305 runs on the CPU's vector unit on every platform broca builds for, about
/// three times the speed of the RustCrypto software path measured on Apple silicon (1.8 against
/// 0.6 GB/s), and decrypting every frame a session read touches is most of what a restart spends
/// its CPU on. The bytes are the same as the RustCrypto AEAD's, so units sealed by either read
/// with the other.
#[derive(Clone)]
struct XChaCha {
    key: [u8; 32],
}

impl XChaCha {
    /// The RFC 8439 key and nonce that seal a unit under the 24-byte `nonce`. Each unit's nonce
    /// is random, so its subkey is unique to it.
    fn subkey(&self, nonce: &[u8; NONCE_LEN]) -> (ring::aead::LessSafeKey, ring::aead::Nonce) {
        use chacha20::cipher::{consts::U10, generic_array::GenericArray};
        let subkey = chacha20::hchacha::<U10>(
            GenericArray::from_slice(&self.key),
            GenericArray::from_slice(&nonce[..16]),
        );
        let key = ring::aead::UnboundKey::new(&ring::aead::CHACHA20_POLY1305, &subkey)
            .expect("HChaCha20 derives a 32-byte key");
        let mut short = [0u8; 12];
        short[4..].copy_from_slice(&nonce[16..]);
        (
            ring::aead::LessSafeKey::new(key),
            ring::aead::Nonce::assume_unique_for_key(short),
        )
    }

    fn seal(&self, nonce: &[u8; NONCE_LEN], aad: &[u8], buffer: &mut [u8]) -> [u8; TAG_LEN] {
        let (key, nonce) = self.subkey(nonce);
        let tag = key
            .seal_in_place_separate_tag(nonce, ring::aead::Aad::from(aad), buffer)
            .expect("encryption of a bounded unit cannot fail");
        tag.as_ref()
            .try_into()
            .expect("a ChaCha20-Poly1305 tag is 16 bytes")
    }

    /// Decrypts `buffer` in place, or `None` when it fails to authenticate under `aad` and `tag`.
    fn open(
        &self,
        nonce: &[u8; NONCE_LEN],
        aad: &[u8],
        buffer: &mut [u8],
        tag: &[u8; TAG_LEN],
    ) -> Option<()> {
        let (key, nonce) = self.subkey(nonce);
        key.open_in_place_separate_tag(
            nonce,
            ring::aead::Aad::from(aad),
            ring::aead::Tag::from(*tag),
            buffer,
            0..,
        )
        .ok()
        .map(drop)
    }
}

/// The check a plain unit carries: the first 16 bytes of SHA-256 over a fixed context, the
/// length of the authenticated data, the authenticated data and the plaintext. It covers what an
/// encrypted unit's tag covers, so torn bytes, damage and a unit moved to another place fail it,
/// but it holds no key, so it can't detect a deliberate edit.
pub(crate) fn plain_check(aad: &[u8], plaintext: &[u8]) -> [u8; CHECK_LEN] {
    let digest = Sha256::new()
        .chain_update(CHECK_CONTEXT)
        .chain_update((aad.len() as u64).to_le_bytes())
        .chain_update(aad)
        .chain_update(plaintext)
        .finalize();
    digest[..CHECK_LEN]
        .try_into()
        .expect("a SHA-256 digest is longer than the check")
}

impl FrameKey {
    pub fn new(id: u16, key: &[u8; 32]) -> Self {
        Self {
            id,
            cipher: Some(XChaCha { key: *key }),
        }
    }

    /// The key of a plain session's units, under key id [`crate::keys::PLAIN_KEY_ID`].
    pub fn plain() -> Self {
        Self {
            id: crate::keys::PLAIN_KEY_ID,
            cipher: None,
        }
    }

    /// A key in mode `cipher` under key id `id`: `key` encrypts in the encrypted mode and is
    /// ignored in the plain one. Tests and fuzz targets only, so one test runs in either mode.
    #[cfg(any(test, fuzzing))]
    pub fn for_tests(cipher: Cipher, id: u16, key: &[u8; 32]) -> Self {
        match cipher {
            Cipher::XChaCha20Poly1305 => Self::new(id, key),
            Cipher::None => Self { id, cipher: None },
        }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    /// The mode this key seals in.
    pub fn cipher(&self) -> Cipher {
        match self.cipher {
            Some(_) => Cipher::XChaCha20Poly1305,
            None => Cipher::None,
        }
    }

    /// Bytes a sealed unit adds to its plaintext.
    pub fn unit_overhead(&self) -> usize {
        match self.cipher {
            Some(_) => NONCE_LEN + TAG_LEN,
            None => CHECK_LEN,
        }
    }

    /// Bytes a frame sealed under this key adds around its record body.
    pub fn frame_overhead(&self) -> usize {
        LENGTH_LEN + HEADER_LEN + CRC_LEN + self.unit_overhead()
    }

    /// Seals `plaintext` as one unit. Encrypted: a random nonce, the ciphertext and the tag.
    /// Plain: the plaintext, then its check (see `plain_check`). `aad` names where the unit
    /// lives; [`open_unit`](Self::open_unit) must get the same bytes.
    pub(crate) fn seal_unit(&self, aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
        let Some(cipher) = &self.cipher else {
            let mut out = Vec::with_capacity(plaintext.len() + CHECK_LEN);
            out.extend_from_slice(plaintext);
            out.extend_from_slice(&plain_check(aad, plaintext));
            return out;
        };
        let mut nonce = [0u8; NONCE_LEN];
        crate::keys::random_bytes(&mut nonce);
        let mut buffer = plaintext.to_vec();
        let tag = cipher.seal(&nonce, aad, &mut buffer);
        let mut out = Vec::with_capacity(NONCE_LEN + buffer.len() + TAG_LEN);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&buffer);
        out.extend_from_slice(&tag);
        out
    }

    /// The plaintext of a unit [`seal_unit`](Self::seal_unit) produced under this key, or `None`
    /// when it fails to authenticate, or fails its check, under `aad`. A unit is only ever read
    /// in this key's mode.
    pub(crate) fn open_unit(&self, aad: &[u8], sealed: &[u8]) -> Option<Vec<u8>> {
        let Some(cipher) = &self.cipher else {
            let body_len = sealed.len().checked_sub(CHECK_LEN)?;
            let (body, check) = sealed.split_at(body_len);
            return (check == plain_check(aad, body)).then(|| body.to_vec());
        };
        if sealed.len() < NONCE_LEN + TAG_LEN {
            return None;
        }
        let (nonce, rest) = sealed.split_at(NONCE_LEN);
        let (ciphertext, tag) = rest.split_at(rest.len() - TAG_LEN);
        let mut buffer = ciphertext.to_vec();
        cipher.open(
            nonce.try_into().ok()?,
            aad,
            &mut buffer,
            tag.try_into().ok()?,
        )?;
        Some(buffer)
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

/// Seals `body` into a frame. `bind` names where the frame lives (session id and log name);
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
    let length = (HEADER_LEN + CRC_LEN + key.unit_overhead() + body.len()) as u32;
    let mut out = Vec::with_capacity(LENGTH_LEN + length as usize);
    out.extend_from_slice(&length.to_le_bytes());
    write_header(header, &mut out)?;
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());

    let mut aad = Vec::with_capacity(bind.len() + LENGTH_LEN + HEADER_LEN);
    aad.extend_from_slice(bind);
    aad.extend_from_slice(&out[..LENGTH_LEN + HEADER_LEN]);
    out.extend_from_slice(&key.seal_unit(&aad, body));
    Ok(out)
}

/// Reads and opens the frame at the start of `bytes`. `key` finds a data key by its id.
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
    // The smallest frame is the one its key seals around an empty body. A key id this reader
    // doesn't hold refuses below, after the length is judged as an encrypted frame's.
    let unit_overhead = key(header.key_id).map_or(NONCE_LEN + TAG_LEN, FrameKey::unit_overhead);
    let smallest = HEADER_LEN + CRC_LEN + unit_overhead;
    if !(smallest..=smallest + MAX_BODY).contains(&length) {
        return Err(FrameError::BadHeader("length out of range"));
    }
    let total = LENGTH_LEN + length;
    if bytes.len() < total {
        return Ok(FrameRead::Incomplete);
    }
    let cipher = key(header.key_id).ok_or(FrameError::UnknownKey(header.key_id))?;
    let mut aad = Vec::with_capacity(bind.len() + LENGTH_LEN + HEADER_LEN);
    aad.extend_from_slice(bind);
    aad.extend_from_slice(&bytes[..LENGTH_LEN + HEADER_LEN]);
    let body = cipher
        .open_unit(&aad, &bytes[fixed..total])
        .ok_or(FrameError::Authentication)?;
    Ok(FrameRead::Frame {
        header,
        body,
        len: total,
    })
}

#[cfg(test)]
mod tests;
