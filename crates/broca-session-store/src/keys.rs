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
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

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

pub(crate) fn random_bytes(bytes: &mut [u8]) {
    #[cfg(test)]
    if test_entropy::fill(bytes) {
        return;
    }
    getrandom::getrandom(bytes).expect("the OS random source is available");
}

#[cfg(test)]
pub(crate) mod test_entropy {
    use sha2::{Digest, Sha256};
    use std::cell::RefCell;
    thread_local! { static COUNTER: RefCell<Option<u64>> = const { RefCell::new(None) }; }

    pub(super) fn fill(bytes: &mut [u8]) -> bool {
        COUNTER.with_borrow_mut(|counter| {
            let Some(counter) = counter else {
                return false;
            };
            for chunk in bytes.chunks_mut(32) {
                let digest = Sha256::digest(counter.to_le_bytes());
                chunk.copy_from_slice(&digest[..chunk.len()]);
                *counter += 1;
            }
            true
        })
    }

    pub(crate) fn replay<T>(build: impl FnOnce() -> T) -> T {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                COUNTER.set(None);
            }
        }
        COUNTER.set(Some(1));
        let _reset = Reset;
        build()
    }
}

/// A random 128-bit id from the OS random source.
pub fn random_id() -> Id128 {
    let mut id = [0u8; 16];
    random_bytes(&mut id);
    Id128(id)
}

/// The deployment keys available to this store. New encrypted data uses the configured id,
/// or the lowest loaded id in the current read grants if no id was configured. Existing data
/// selects the key that originally locked it.
///
/// Before the key has loaded from the vault, [`DeploymentKey::unloaded`] stands in for it: plain
/// sessions open and are created with it, and every encrypted session refuses.
pub struct DeploymentKey {
    configured_id: Option<u16>,
    keys: RwLock<AvailableKeys>,
    /// The mode of sessions created with this key. Sessions that exist keep their own.
    new_sessions: Cipher,
}

#[derive(Default)]
struct AvailableKeys {
    granted: BTreeSet<u16>,
    loaded: BTreeMap<u16, Arc<LoadedKey>>,
}

struct LoadedKey {
    cipher: XChaCha20Poly1305,
    bytes: [u8; 32],
    setup: Arc<crate::setup::SetupKey>,
}

impl DeploymentKey {
    /// Deployment key `id`, loaded. Sessions created with it are encrypted.
    pub fn new(id: u16, key: &[u8; 32]) -> Self {
        Self {
            configured_id: Some(id),
            keys: RwLock::new(AvailableKeys {
                granted: BTreeSet::from([id]),
                loaded: BTreeMap::from([(
                    id,
                    Arc::new(LoadedKey {
                        cipher: XChaCha20Poly1305::new(Key::from_slice(key)),
                        bytes: *key,
                        setup: Arc::new(crate::setup::SetupKey::new(
                            id,
                            &crate::prune::keyed_digest(key, b"broca-setup-encrypt-v1").0,
                            crate::prune::keyed_digest(key, b"broca-setup-name-v1").0,
                        )),
                    }),
                )]),
            }),
            new_sessions: Cipher::XChaCha20Poly1305,
        }
    }

    /// A ring without any loaded keys. With no configured id it follows the read grants.
    /// Only plain sessions open with it at first, and the sessions created with it are plain.
    pub fn unloaded(configured_id: impl Into<Option<u16>>) -> Self {
        Self {
            configured_id: configured_id.into(),
            keys: RwLock::default(),
            new_sessions: Cipher::None,
        }
    }

    /// An owned copy for one conversion worker, dropped when that worker ends. This is
    /// explicit rather than `Clone` so ordinary callers do not copy key material by accident.
    pub fn duplicate_for_worker(&self) -> Self {
        let keys = self.keys.read().unwrap();
        Self {
            configured_id: self.configured_id,
            new_sessions: self.new_sessions,
            keys: RwLock::new(AvailableKeys {
                granted: keys.granted.clone(),
                loaded: keys.loaded.clone(),
            }),
        }
    }

    /// This key, creating new sessions in mode `cipher`. An unloaded key can't create encrypted
    /// sessions: creating one refuses.
    pub fn creating(mut self, cipher: Cipher) -> Self {
        self.new_sessions = cipher;
        self
    }

    pub fn configured_id(&self) -> Option<u16> {
        self.configured_id
    }

    /// The preferred id, or the plain key id when there is no choice yet. For reporting or
    /// deciding whether new encrypted data can be written, use [`Self::write_key_state`].
    pub fn id(&self) -> u16 {
        self.write_key_state().0.unwrap_or(PLAIN_KEY_ID)
    }

    /// The write key's id and whether it is loaded, from the same snapshot. A configured id
    /// is reported even if it has not loaded yet.
    pub fn write_key_state(&self) -> (Option<u16>, bool) {
        let keys = self.keys.read().unwrap();
        let id = keys.write_id(self.configured_id);
        (id, id.is_some_and(|id| keys.loaded.contains_key(&id)))
    }

    /// Whether the write key has loaded, so new encrypted data can be written.
    pub fn is_loaded(&self) -> bool {
        self.write_key_state().1
    }

    pub fn has_key(&self, id: u16) -> bool {
        self.keys.read().unwrap().loaded.contains_key(&id)
    }

    pub fn has_loaded_keys(&self) -> bool {
        !self.keys.read().unwrap().loaded.is_empty()
    }

    fn loaded_key(&self, id: u16) -> Option<Arc<LoadedKey>> {
        self.keys.read().unwrap().loaded.get(&id).cloned()
    }

    fn write_key(&self) -> Option<(u16, Arc<LoadedKey>)> {
        let keys = self.keys.read().unwrap();
        let id = keys.write_id(self.configured_id)?;
        Some((id, Arc::clone(keys.loaded.get(&id)?)))
    }

    /// Records the latest scoped listing, including keys whose reads have not succeeded yet.
    pub fn set_granted(&self, ids: BTreeSet<u16>) {
        self.keys.write().unwrap().granted = ids;
    }

    pub fn has_pending_keys(&self) -> bool {
        self.keys.read().unwrap().has_pending()
    }

    /// Installs a key read from the vault. Deployment records are immutable by id; a later
    /// load never replaces a key already in use.
    pub fn install(&self, key: DeploymentKey) -> bool {
        let offered = key.keys.into_inner().unwrap();
        let mut keys = self.keys.write().unwrap();
        let before = keys.loaded.len();
        for (id, loaded) in offered.loaded {
            keys.loaded.entry(id).or_insert(loaded);
        }
        keys.loaded.len() != before
    }

    /// A snapshot of the setup name spaces to try, and whether a granted key is still missing.
    pub(crate) fn setup_keys(&self, cipher: Cipher) -> (Vec<Arc<crate::setup::SetupKey>>, bool) {
        if cipher == Cipher::None {
            return (vec![Arc::new(crate::setup::SetupKey::plain())], false);
        }
        let keys = self.keys.read().unwrap();
        let preferred = keys.write_id(self.configured_id);
        let loaded = preferred
            .and_then(|id| keys.loaded.get(&id))
            .into_iter()
            .chain(
                keys.loaded
                    .iter()
                    .filter_map(|(id, key)| (Some(*id) != preferred).then_some(key)),
            )
            .map(|key| Arc::clone(&key.setup))
            .collect();
        (loaded, keys.has_pending())
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
            Cipher::XChaCha20Poly1305 => self.write_key().map(|(_, key)| (*key.setup).clone()),
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
    pub(crate) fn expiry_key(&self, cipher: Cipher) -> Option<(u16, FrameKey)> {
        match cipher {
            Cipher::None => Some((self.id(), FrameKey::plain())),
            Cipher::XChaCha20Poly1305 => self
                .write_key()
                .map(|(id, key)| (id, expiry_frame_key(id, &key))),
        }
    }

    pub(crate) fn expiry_key_for(&self, cipher: Cipher, id: u16) -> Option<FrameKey> {
        match cipher {
            Cipher::None => Some(FrameKey::plain()),
            Cipher::XChaCha20Poly1305 => self.loaded_key(id).map(|key| expiry_frame_key(id, &key)),
        }
    }
}

impl AvailableKeys {
    fn write_id(&self, configured_id: Option<u16>) -> Option<u16> {
        configured_id.or_else(|| {
            self.granted
                .iter()
                .copied()
                .find(|id| self.loaded.contains_key(id))
        })
    }

    fn has_pending(&self) -> bool {
        self.granted.iter().any(|id| !self.loaded.contains_key(id))
    }
}

fn expiry_frame_key(id: u16, key: &LoadedKey) -> FrameKey {
    FrameKey::new(
        id,
        &crate::prune::keyed_digest(&key.bytes, b"broca-expired-encrypt-v1").0,
    )
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
        random_bytes(&mut bytes);
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

fn wrap_aad(deployment_id: u16, session: &Id128, key_id: u16) -> Vec<u8> {
    let mut aad = Vec::with_capacity(WRAP_CONTEXT.len() + 2 + 16 + 2);
    aad.extend_from_slice(WRAP_CONTEXT);
    aad.extend_from_slice(&deployment_id.to_le_bytes());
    aad.extend_from_slice(&session.0);
    aad.extend_from_slice(&key_id.to_le_bytes());
    aad
}

/// Wraps `key` for session `session` under the deployment key: nonce, ciphertext and tag.
/// `None` when the deployment key hasn't loaded.
pub fn wrap(deployment: &DeploymentKey, session: &Id128, key: &DataKey) -> Option<Vec<u8>> {
    wrap_with_id(deployment, session, key).map(|(_, wrapped)| wrapped)
}

/// Wraps the data key and returns the id of the deployment key that actually wrapped it.
/// Reading the preferred id separately could race a change in the vault listing.
pub(crate) fn wrap_with_id(
    deployment: &DeploymentKey,
    session: &Id128,
    key: &DataKey,
) -> Option<(u16, Vec<u8>)> {
    let (deployment_id, loaded) = deployment.write_key()?;
    let mut nonce = [0u8; NONCE_LEN];
    random_bytes(&mut nonce);
    let mut buffer = key.bytes.to_vec();
    let tag = loaded
        .cipher
        .encrypt_in_place_detached(
            XNonce::from_slice(&nonce),
            &wrap_aad(deployment_id, session, key.id),
            &mut buffer,
        )
        .expect("encrypting 32 bytes cannot fail");
    let mut out = nonce.to_vec();
    out.extend_from_slice(&buffer);
    out.extend_from_slice(&tag);
    Some((deployment_id, out))
}

/// Unwraps a key [`wrap`] produced for this session and key id.
pub fn unwrap(
    deployment: &DeploymentKey,
    session: &Id128,
    key_id: u16,
    wrapped: &[u8],
) -> Result<DataKey, UnwrapError> {
    let id = deployment
        .write_key_state()
        .0
        .ok_or(UnwrapError::NotLoaded)?;
    unwrap_with_id(deployment, id, session, key_id, wrapped)
}

pub(crate) fn unwrap_with_id(
    deployment: &DeploymentKey,
    deployment_id: u16,
    session: &Id128,
    key_id: u16,
    wrapped: &[u8],
) -> Result<DataKey, UnwrapError> {
    let loaded = deployment
        .loaded_key(deployment_id)
        .ok_or(UnwrapError::NotLoaded)?;
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
            &wrap_aad(deployment_id, session, key_id),
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
    random_bytes(&mut nonce);
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
    fn a_worker_key_copy_keeps_identity_mode_and_wrapping_material() {
        for loaded in [false, true] {
            for mode in [Cipher::None, Cipher::XChaCha20Poly1305] {
                let original = if loaded {
                    DeploymentKey::new(7, &[42; 32])
                } else {
                    DeploymentKey::unloaded(7)
                }
                .creating(mode);
                let copy = original.duplicate_for_worker();
                drop(original);
                assert_eq!(copy.id(), 7);
                assert_eq!(copy.is_loaded(), loaded);
                assert_eq!(copy.new_sessions(), mode);
                let data = DataKey::from_bytes(7, [13; 32]);
                let session = Id128([11; 16]);
                if loaded {
                    let wrapped = wrap(&copy, &session, &data).unwrap();
                    let independent = DeploymentKey::new(7, &[42; 32]);
                    let opened = unwrap(&independent, &session, 7, &wrapped).unwrap();
                    assert_eq!(opened.derive(b"worker-copy"), data.derive(b"worker-copy"));
                } else {
                    assert!(wrap(&copy, &session, &data).is_none());
                }
            }
        }
    }

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
