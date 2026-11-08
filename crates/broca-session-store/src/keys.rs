//! Session keys and how they are stored.
//!
//! Each session has a random 256-bit data key, minted with the session. Every frame, block and
//! object of the session is encrypted under it. The data key is stored only in the session head,
//! wrapped (encrypted) under the deployment key, which comes from the vault and never touches
//! storage. Without the deployment key a session can't be opened, and there is no fallback to
//! plaintext.
//!
//! A wrapped key binds the session id and the key id in its authenticated data, so a wrapped key
//! copied into another session's head, or under another id, fails to unwrap.

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{Key, Tag, XChaCha20Poly1305, XNonce};

use crate::codec::Id128;
use crate::frame::FrameKey;

const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;
const WRAP_CONTEXT: &[u8] = b"broca-session-key-v1";

/// A random 128-bit id from the OS random source.
pub fn random_id() -> Id128 {
    let mut id = [0u8; 16];
    getrandom::getrandom(&mut id).expect("the OS random source is available");
    Id128(id)
}

/// The deployment key: the key that wraps every session's data key. `id` names it, so a
/// rotation can tell keys apart.
pub struct DeploymentKey {
    id: u16,
    cipher: XChaCha20Poly1305,
    bytes: [u8; 32],
}

impl DeploymentKey {
    pub fn new(id: u16, key: &[u8; 32]) -> Self {
        Self {
            id,
            cipher: XChaCha20Poly1305::new(Key::from_slice(key)),
            bytes: *key,
        }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    /// The keys of the deployment's setup store, derived from this key for that purpose only.
    pub fn setup_key(&self) -> crate::setup::SetupKey {
        crate::setup::SetupKey::new(
            self.id,
            &crate::prune::keyed_digest(&self.bytes, b"broca-setup-encrypt-v1").0,
            crate::prune::keyed_digest(&self.bytes, b"broca-setup-name-v1").0,
        )
    }
}

/// A session's data key in the clear, held only in memory.
#[derive(Clone)]
pub struct DataKey {
    pub id: u16,
    bytes: [u8; 32],
}

impl DataKey {
    /// Mints a new random data key under key id `id`.
    pub fn generate(id: u16) -> Self {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).expect("the OS random source is available");
        Self { id, bytes }
    }

    /// A data key with fixed bytes, so tests and fuzz targets are deterministic. Never in a
    /// shipping build.
    #[cfg(any(test, fuzzing))]
    pub fn from_bytes(id: u16, bytes: [u8; 32]) -> Self {
        Self { id, bytes }
    }

    /// The frame key that encrypts the session's frames under this data key.
    pub fn frame_key(&self) -> FrameKey {
        FrameKey::new(self.id, &self.bytes)
    }

    /// A key for one purpose, named by `context`, derived from this one by HMAC-SHA256, so keys
    /// derived for different purposes are unrelated.
    pub fn derive(&self, context: &[u8]) -> [u8; 32] {
        crate::prune::keyed_digest(&self.bytes, context).0
    }

    /// The key subscribe cursors of this session are digested under.
    pub fn cursor_key(&self) -> CursorKey {
        CursorKey(self.derive(CURSOR_CONTEXT))
    }
}

/// What a subscribe cursor's key is derived for, so no other key derived from the data key
/// equals it.
const CURSOR_CONTEXT: &[u8] = b"broca-subscribe-cursor-v1";

/// The key a subscribe cursor's digest is made with: derived from the session's data key, so
/// a cursor proves which record it names without letting anyone holding it guess a short
/// stored value from the digest.
#[derive(Clone)]
pub struct CursorKey([u8; 32]);

impl CursorKey {
    /// The digest a cursor naming `record` carries: the first 8 bytes of HMAC-SHA256 over the
    /// record's canonical encoding (its kind number, little-endian, then its body), read as a
    /// big-endian number.
    pub fn digest(&self, record: &crate::record::Record) -> u64 {
        let mut encoding = record.kind().to_le_bytes().to_vec();
        encoding.extend(record.encode_body());
        let digest = crate::prune::keyed_digest(&self.0, &encoding);
        u64::from_be_bytes(digest.0[..8].try_into().expect("a digest has 32 bytes"))
    }
}

/// Why a wrapped key couldn't be unwrapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnwrapError {
    /// Not the length of a wrapped key.
    Malformed,
    /// The deployment key, the session id or the key id doesn't match what it was wrapped with,
    /// or the bytes were changed.
    Authentication,
}

fn wrap_aad(deployment: &DeploymentKey, session: &Id128, key_id: u16) -> Vec<u8> {
    let mut aad = Vec::with_capacity(WRAP_CONTEXT.len() + 2 + 16 + 2);
    aad.extend_from_slice(WRAP_CONTEXT);
    aad.extend_from_slice(&deployment.id.to_le_bytes());
    aad.extend_from_slice(&session.0);
    aad.extend_from_slice(&key_id.to_le_bytes());
    aad
}

/// Wraps `key` for session `session` under the deployment key: nonce, ciphertext and tag.
pub fn wrap(deployment: &DeploymentKey, session: &Id128, key: &DataKey) -> Vec<u8> {
    let mut nonce = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce).expect("the OS random source is available");
    let mut buffer = key.bytes.to_vec();
    let tag = deployment
        .cipher
        .encrypt_in_place_detached(
            XNonce::from_slice(&nonce),
            &wrap_aad(deployment, session, key.id),
            &mut buffer,
        )
        .expect("encrypting 32 bytes cannot fail");
    let mut out = nonce.to_vec();
    out.extend_from_slice(&buffer);
    out.extend_from_slice(&tag);
    out
}

/// Unwraps a key [`wrap`] produced for this session and key id.
pub fn unwrap(
    deployment: &DeploymentKey,
    session: &Id128,
    key_id: u16,
    wrapped: &[u8],
) -> Result<DataKey, UnwrapError> {
    if wrapped.len() != NONCE_LEN + 32 + TAG_LEN {
        return Err(UnwrapError::Malformed);
    }
    let (nonce, rest) = wrapped.split_at(NONCE_LEN);
    let (ciphertext, tag) = rest.split_at(32);
    let mut buffer = ciphertext.to_vec();
    deployment
        .cipher
        .decrypt_in_place_detached(
            XNonce::from_slice(nonce),
            &wrap_aad(deployment, session, key_id),
            &mut buffer,
            Tag::from_slice(tag),
        )
        .map_err(|_| UnwrapError::Authentication)?;
    let bytes: [u8; 32] = buffer.try_into().map_err(|_| UnwrapError::Malformed)?;
    Ok(DataKey { id: key_id, bytes })
}

/// A tag that authenticates `data` under the data key without encrypting it: the head is
/// readable without the key (it names where things are), but nobody without the key can change
/// it undetected. `context` separates what is being authenticated.
pub fn authenticate(key: &DataKey, context: &[u8], data: &[u8]) -> Vec<u8> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key.bytes));
    let mut nonce = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce).expect("the OS random source is available");
    let mut aad = context.to_vec();
    aad.extend_from_slice(data);
    let tag = cipher
        .encrypt_in_place_detached(XNonce::from_slice(&nonce), &aad, &mut Vec::new())
        .expect("authenticating cannot fail");
    let mut out = nonce.to_vec();
    out.extend_from_slice(&tag);
    out
}

/// Checks a tag [`authenticate`] produced.
pub fn verify(key: &DataKey, context: &[u8], data: &[u8], tag: &[u8]) -> bool {
    if tag.len() != NONCE_LEN + TAG_LEN {
        return false;
    }
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&key.bytes));
    let (nonce, tag) = tag.split_at(NONCE_LEN);
    let mut aad = context.to_vec();
    aad.extend_from_slice(data);
    cipher
        .decrypt_in_place_detached(
            XNonce::from_slice(nonce),
            &aad,
            &mut Vec::new(),
            Tag::from_slice(tag),
        )
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wrapped_key_unwraps_only_for_its_session_and_id() {
        let deployment = DeploymentKey::new(1, &[3; 32]);
        let session = random_id();
        let key = DataKey::generate(1);
        let wrapped = wrap(&deployment, &session, &key);
        let back = unwrap(&deployment, &session, 1, &wrapped).unwrap();
        assert_eq!(back.bytes, key.bytes);
        assert_eq!(
            unwrap(&deployment, &random_id(), 1, &wrapped).err(),
            Some(UnwrapError::Authentication)
        );
        assert_eq!(
            unwrap(&deployment, &session, 2, &wrapped).err(),
            Some(UnwrapError::Authentication)
        );
        let other = DeploymentKey::new(1, &[4; 32]);
        assert_eq!(
            unwrap(&other, &session, 1, &wrapped).err(),
            Some(UnwrapError::Authentication)
        );
    }

    #[test]
    fn a_tag_fails_when_the_data_or_context_changes() {
        let key = DataKey::generate(1);
        let tag = authenticate(&key, b"head", b"bytes");
        assert!(verify(&key, b"head", b"bytes", &tag));
        assert!(!verify(&key, b"head", b"bytez", &tag));
        assert!(!verify(&key, b"frame", b"bytes", &tag));
        assert!(!verify(&DataKey::generate(1), b"head", b"bytes", &tag));
    }

    /// HMAC-SHA256 as RFC 2104 defines it, for a key of at most one block.
    fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
        use sha2::{Digest as _, Sha256};
        let mut block = [0u8; 64];
        block[..key.len()].copy_from_slice(key);
        let inner = Sha256::new()
            .chain_update(block.map(|byte| byte ^ 0x36))
            .chain_update(data)
            .finalize();
        Sha256::new()
            .chain_update(block.map(|byte| byte ^ 0x5c))
            .chain_update(inner)
            .finalize()
            .into()
    }

    /// A subscribe cursor's digest is the first 8 bytes of HMAC-SHA256 over its record's kind
    /// and body, under a key derived from the session's data key for cursors alone: another
    /// session's key, or the data key itself, digests the same record differently.
    #[test]
    fn a_cursor_digest_is_a_keyed_hmac_of_its_record() {
        // RFC 4231 test case 2 pins the construction itself.
        assert_eq!(
            crate::codec::hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        let key = DataKey::generate(1);
        let record = crate::record::Record::Event(crate::record::Event {
            name: "a".into(),
            run_id: None,
            body: None,
        });
        let derived = hmac_sha256(&key.bytes, b"broca-subscribe-cursor-v1");
        let mut encoding = record.kind().to_le_bytes().to_vec();
        encoding.extend(record.encode_body());
        let full = hmac_sha256(&derived, &encoding);
        let expected = u64::from_be_bytes(full[..8].try_into().unwrap());
        assert_eq!(key.cursor_key().digest(&record), expected);
        assert_ne!(DataKey::generate(1).cursor_key().digest(&record), expected);
        let other = crate::record::Record::Event(crate::record::Event {
            name: "b".into(),
            run_id: None,
            body: None,
        });
        assert_ne!(key.cursor_key().digest(&other), expected);
    }
}
