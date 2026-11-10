//! Session keys and how they are stored.
//!
//! A session's **cipher mode** is chosen once, when the session is created, and recorded in its
//! head ([`Cipher`]). It never changes, and a reader never infers it: a head without the field is
//! encrypted.
//!
//! An encrypted session has a random 256-bit data key, minted with the session. Every frame, block
//! and object of the session is encrypted under it. The data key is stored only in the session
//! head, wrapped (encrypted) under the deployment key, which comes from the vault and never touches
//! storage. Without the deployment key an encrypted session can't be opened, and there is no
//! fallback to plaintext.
//!
//! A plain session has no data key. Its units are stored as plaintext followed by an unkeyed check
//! (see `frame::FrameKey::seal_unit`), and every name or digest an encrypted session derives from
//! its data key is derived from the [`published_key`] instead. A plain session opens without the
//! deployment key.
//!
//! A wrapped key binds the session id and the key id in its authenticated data, so a wrapped key
//! copied into another session's head, or under another id, fails to unwrap.

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{Key, Tag, XChaCha20Poly1305, XNonce};
use sha2::{Digest as _, Sha256};

use crate::codec::Id128;
use crate::frame::FrameKey;

const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;
const WRAP_CONTEXT: &[u8] = b"broca-session-key-v1";

crate::values! {
    /// How a session's objects are stored. Chosen when the session is created and recorded in its
    /// head; it never changes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Cipher {
        /// Encrypted under the session's data key, which the deployment key wraps.
        1 XChaCha20Poly1305,
        /// Plain: each unit is its plaintext followed by an unkeyed check. A torn, damaged or
        /// moved unit fails its check, as an encrypted one fails its tag, but nothing is secret and
        /// a deliberate edit isn't detected.
        2 None,
    }
}

impl Cipher {
    /// Every mode, encrypted first: what tests run each case in.
    pub const ALL: [Cipher; 2] = [Cipher::XChaCha20Poly1305, Cipher::None];

    /// The name health and the configuration use for the mode of new sessions.
    pub fn label(self) -> &'static str {
        match self {
            Cipher::XChaCha20Poly1305 => "encrypted",
            Cipher::None => "plain",
        }
    }
}

/// The key id a plain session's frames and units name where an encrypted session names its data
/// key's id. A plain session has no data key, and no data key ever has this id.
pub const PLAIN_KEY_ID: u16 = 0;

/// What the published key is the SHA-256 of.
const PUBLISHED_KEY_CONTEXT: &[u8] = b"broca-plain-published-key-v1";

/// The fixed key plain sessions derive names and digests from wherever an encrypted session
/// derives them from its data key or the deployment key: media and id-index names, setup part
/// names, subscribe cursors and pruning digests. It is published (this is its definition), so a
/// name derived from it is a plain content hash that hides nothing.
pub fn published_key() -> [u8; 32] {
    Sha256::digest(PUBLISHED_KEY_CONTEXT).into()
}

/// A random 128-bit id from the OS random source.
pub fn random_id() -> Id128 {
    let mut id = [0u8; 16];
    getrandom::getrandom(&mut id).expect("the OS random source is available");
    Id128(id)
}

/// The deployment key: the key that wraps every encrypted session's data key. `id` names it, so
/// a rotation can tell keys apart. It also says which mode the sessions created with it are
/// stored in ([`DeploymentKey::creating`]).
///
/// Before the key has loaded from the vault, [`DeploymentKey::unloaded`] stands in for it: plain
/// sessions open and are created with it, and every encrypted session refuses.
pub struct DeploymentKey {
    id: u16,
    /// The key itself, once it has loaded.
    loaded: Option<LoadedKey>,
    /// The mode of sessions created with this key. Sessions that exist keep their own.
    new_sessions: Cipher,
}

struct LoadedKey {
    cipher: XChaCha20Poly1305,
    bytes: [u8; 32],
}

impl DeploymentKey {
    /// Deployment key `id`, loaded. Sessions created with it are encrypted.
    pub fn new(id: u16, key: &[u8; 32]) -> Self {
        Self {
            id,
            loaded: Some(LoadedKey {
                cipher: XChaCha20Poly1305::new(Key::from_slice(key)),
                bytes: *key,
            }),
            new_sessions: Cipher::XChaCha20Poly1305,
        }
    }

    /// Deployment key `id` before it has loaded. Only plain sessions open with it, and the
    /// sessions created with it are plain.
    pub fn unloaded(id: u16) -> Self {
        Self {
            id,
            loaded: None,
            new_sessions: Cipher::None,
        }
    }

    /// This key, creating new sessions in mode `cipher`. An unloaded key can't create encrypted
    /// sessions: creating one refuses.
    pub fn creating(mut self, cipher: Cipher) -> Self {
        self.new_sessions = cipher;
        self
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    /// Whether the key itself has loaded, so encrypted sessions open with it.
    pub fn is_loaded(&self) -> bool {
        self.loaded.is_some()
    }

    /// The mode of the sessions created with this key.
    pub fn new_sessions(&self) -> Cipher {
        self.new_sessions
    }

    /// The keys of the deployment's setup store for sessions in mode `cipher`. Encrypted
    /// sessions' are derived from this key for that purpose only, so they need it loaded; plain
    /// sessions' come from the published key, in a name space of their own.
    pub fn setup_key_for(&self, cipher: Cipher) -> Option<crate::setup::SetupKey> {
        match cipher {
            Cipher::None => Some(crate::setup::SetupKey::plain()),
            Cipher::XChaCha20Poly1305 => self.loaded.as_ref().map(|loaded| {
                crate::setup::SetupKey::new(
                    self.id,
                    &crate::prune::keyed_digest(&loaded.bytes, b"broca-setup-encrypt-v1").0,
                    crate::prune::keyed_digest(&loaded.bytes, b"broca-setup-name-v1").0,
                )
            }),
        }
    }

    /// The keys of the deployment's setup store for encrypted sessions.
    ///
    /// # Panics
    ///
    /// On a key that hasn't loaded; use [`setup_key_for`](Self::setup_key_for) where it may not
    /// have.
    pub fn setup_key(&self) -> crate::setup::SetupKey {
        self.setup_key_for(Cipher::XChaCha20Poly1305)
            .expect("the deployment key has loaded")
    }

    /// The expiry sidecar's unit key, separate from the keys used for session content and setup.
    pub(crate) fn expiry_key(&self, cipher: Cipher) -> Option<FrameKey> {
        match cipher {
            Cipher::None => Some(FrameKey::plain()),
            Cipher::XChaCha20Poly1305 => self.loaded.as_ref().map(|loaded| {
                FrameKey::new(
                    self.id,
                    &crate::prune::keyed_digest(&loaded.bytes, b"broca-expired-encrypt-v1").0,
                )
            }),
        }
    }
}

/// A session's data key in the clear, held only in memory. A plain session's "data key" names
/// its mode and holds the published key, so everything derived from it is unkeyed.
#[derive(Clone)]
pub struct DataKey {
    pub id: u16,
    bytes: [u8; 32],
    cipher: Cipher,
}

impl DataKey {
    /// Mints a new random data key under key id `id`.
    pub fn generate(id: u16) -> Self {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).expect("the OS random source is available");
        Self {
            id,
            bytes,
            cipher: Cipher::XChaCha20Poly1305,
        }
    }

    /// The key of a plain session: no secret, key id [`PLAIN_KEY_ID`], and the published key in
    /// place of the data key's bytes.
    pub fn plain() -> Self {
        Self {
            id: PLAIN_KEY_ID,
            bytes: published_key(),
            cipher: Cipher::None,
        }
    }

    /// A data key with fixed bytes, so tests and fuzz targets are deterministic. Never in a
    /// shipping build.
    #[cfg(any(test, fuzzing))]
    pub fn from_bytes(id: u16, bytes: [u8; 32]) -> Self {
        Self {
            id,
            bytes,
            cipher: Cipher::XChaCha20Poly1305,
        }
    }

    /// The mode of the session this key belongs to.
    pub fn cipher(&self) -> Cipher {
        self.cipher
    }

    /// The frame key that seals the session's frames and units under this data key, in its mode.
    pub fn frame_key(&self) -> FrameKey {
        match self.cipher {
            Cipher::XChaCha20Poly1305 => FrameKey::new(self.id, &self.bytes),
            Cipher::None => FrameKey::plain(),
        }
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
    /// The deployment key hasn't loaded.
    NotLoaded,
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
/// `None` when the deployment key hasn't loaded.
pub fn wrap(deployment: &DeploymentKey, session: &Id128, key: &DataKey) -> Option<Vec<u8>> {
    let loaded = deployment.loaded.as_ref()?;
    let mut nonce = [0u8; NONCE_LEN];
    getrandom::getrandom(&mut nonce).expect("the OS random source is available");
    let mut buffer = key.bytes.to_vec();
    let tag = loaded
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
    Some(out)
}

/// Unwraps a key [`wrap`] produced for this session and key id.
pub fn unwrap(
    deployment: &DeploymentKey,
    session: &Id128,
    key_id: u16,
    wrapped: &[u8],
) -> Result<DataKey, UnwrapError> {
    let loaded = deployment.loaded.as_ref().ok_or(UnwrapError::NotLoaded)?;
    if wrapped.len() != NONCE_LEN + 32 + TAG_LEN {
        return Err(UnwrapError::Malformed);
    }
    let (nonce, rest) = wrapped.split_at(NONCE_LEN);
    let (ciphertext, tag) = rest.split_at(32);
    let mut buffer = ciphertext.to_vec();
    loaded
        .cipher
        .decrypt_in_place_detached(
            XNonce::from_slice(nonce),
            &wrap_aad(deployment, session, key_id),
            &mut buffer,
            Tag::from_slice(tag),
        )
        .map_err(|_| UnwrapError::Authentication)?;
    let bytes: [u8; 32] = buffer.try_into().map_err(|_| UnwrapError::Malformed)?;
    Ok(DataKey {
        id: key_id,
        bytes,
        cipher: Cipher::XChaCha20Poly1305,
    })
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
        let wrapped = wrap(&deployment, &session, &key).unwrap();
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
        // A deployment key that hasn't loaded wraps and unwraps nothing.
        let unloaded = DeploymentKey::unloaded(1);
        assert!(wrap(&unloaded, &session, &key).is_none());
        assert_eq!(
            unwrap(&unloaded, &session, 1, &wrapped).err(),
            Some(UnwrapError::NotLoaded)
        );
    }

    /// A plain session's key holds the published key, so what it derives is the same in every
    /// deployment and differs from an encrypted session's, and its frame key seals plainly.
    #[test]
    fn a_plain_key_derives_from_the_published_key() {
        use sha2::{Digest as _, Sha256};
        let expected: [u8; 32] = Sha256::digest(b"broca-plain-published-key-v1").into();
        assert_eq!(published_key(), expected);
        let plain = DataKey::plain();
        assert_eq!(plain.id, PLAIN_KEY_ID);
        assert_eq!(plain.cipher(), Cipher::None);
        assert_eq!(plain.frame_key().cipher(), Cipher::None);
        assert_eq!(plain.frame_key().id(), PLAIN_KEY_ID);
        assert_eq!(
            plain.derive(b"media-object-name"),
            hmac_sha256(&expected, b"media-object-name")
        );
        assert_ne!(
            plain.derive(b"media-object-name"),
            DataKey::generate(1).derive(b"media-object-name")
        );
        assert!(DeploymentKey::unloaded(1)
            .setup_key_for(Cipher::XChaCha20Poly1305)
            .is_none());
        assert!(DeploymentKey::unloaded(1)
            .setup_key_for(Cipher::None)
            .is_some());
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
