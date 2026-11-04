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
}

impl DeploymentKey {
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

    /// The frame key that encrypts the session's frames under this data key.
    pub fn frame_key(&self) -> FrameKey {
        FrameKey::new(self.id, &self.bytes)
    }

    /// A key for one purpose, named by `context`, derived from this one by HMAC-SHA256, so keys
    /// derived for different purposes are unrelated.
    pub fn derive(&self, context: &[u8]) -> [u8; 32] {
        crate::prune::keyed_digest(&self.bytes, context).0
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
}
