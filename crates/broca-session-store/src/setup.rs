//! The setup store: the parts a run is started with, stored once per deployment and shared by
//! every session.
//!
//! A run's setup is everything it is frozen with except the conversation: the tools offered to
//! the model, each tool provider's catalog answer, the system text, the plan, and so on. Each is
//! a **part**, named in records by the plain SHA-256 of its canonical bytes (`PartRef`). The
//! tools, catalog answers and system text are mostly shared by many sessions (many sessions of
//! one project start with the same tools), so each is stored once, not in every run start. So
//! is the render config: the members that belong to one run or one session (the prompt cache key,
//! a Claude Code persona's or Code Assist's identity, ChatGPT's turn ids) stay in the run start's
//! own `render_identity`, so runs on one route share one render config part. Run starts written
//! before that kept the whole config in the part, about 1.5 to 3.9 KB per session or per run.
//!
//! Storage:
//! - **Name:** a part is stored under an HMAC of its digest with the deployment's naming key. A
//!   record's plain digest stays inside encrypted records, and a stored name reveals nothing about
//!   the content to whoever holds the storage. A session moved to another deployment finds its
//!   parts there under that deployment's names.
//! - **Encryption:** each part is encrypted under the deployment's setup key, with its name in
//!   the authenticated data, so a part served under another name fails to open.
//! - **Plain sessions** keep their parts in a name space of their own (names start with
//!   `plain/`), named under a key derived from the published key and stored with an unkeyed
//!   check instead of encrypted (see `SetupKey::plain`). A session reads parts only in its own
//!   mode, so a plain part and an encrypted part never collide or get read in the wrong mode.
//! - **Verification:** a read checks the decrypted bytes against the digest the record names. A
//!   missing or corrupt part is damage to the session that names it, never an absent value.
//! - **Lifetime:** parts are never deleted, not even when the last session naming one is:
//!   deleting one safely would need every session's run starts checked first. The store grows
//!   only when tools, system text, plans or a route's render config change. Storing a part that
//!   is already there changes nothing.
//!
//! A run start that names a part is written only after the part's store returned durable, so a
//! crash can never leave a record naming a part that isn't there.
//!
//! Parts must never carry session content (the conversation): the run's account and routing
//! decisions stay in the run's own record, as its render identity does. Run starts written
//! before the render identity was split out are the one exception, described above.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sha2::{Digest as _, Sha256};

use crate::backend::{BackendError, SessionBackend};
use crate::codec::{hex, Digest};
use crate::frame::FrameKey;
use crate::record::PartRef;

const AAD_CONTEXT: &[u8] = b"broca-setup-part-v1";

/// What the names of plain sessions' parts start with: their name space.
pub const PLAIN_PREFIX: &str = "plain/";

/// Parts the in-memory cache holds before it starts over. The parts most sessions share number
/// in the hundreds, but each older session read adds its own render config (see the module
/// header), so this bounds a cache that would otherwise grow without limit.
const CACHE_CAP: usize = 4096;

/// The deployment's setup-store keys for one cipher mode: one to name parts, one to seal them.
pub struct SetupKey {
    naming: [u8; 32],
    cipher: FrameKey,
}

impl SetupKey {
    pub(crate) fn new(id: u16, encrypt: &[u8; 32], naming: [u8; 32]) -> Self {
        Self {
            naming,
            cipher: FrameKey::new(id, encrypt),
        }
    }

    /// The setup-store keys of plain sessions: parts named under a key derived from the
    /// published key as an encrypted part's naming key is derived from the deployment key, and
    /// sealed with the plain check. The same in every deployment.
    pub fn plain() -> Self {
        Self {
            naming: crate::prune::keyed_digest(
                &crate::keys::published_key(),
                b"broca-setup-name-v1",
            )
            .0,
            cipher: FrameKey::plain(),
        }
    }

    /// The mode of the parts this key stores and reads.
    pub fn cipher(&self) -> crate::keys::Cipher {
        self.cipher.cipher()
    }

    /// The name part `digest` is stored under, in this key's name space.
    pub fn name(&self, digest: &Digest) -> String {
        let name = hex(&crate::prune::keyed_digest(&self.naming, &digest.0).0);
        match self.cipher() {
            crate::keys::Cipher::XChaCha20Poly1305 => name,
            crate::keys::Cipher::None => format!("{PLAIN_PREFIX}{name}"),
        }
    }

    /// Whether a stored name is in this key's name space: plain names carry their prefix, and
    /// encrypted names are bare.
    fn owns(&self, name: &str) -> bool {
        match self.cipher() {
            crate::keys::Cipher::XChaCha20Poly1305 => !name.contains('/'),
            crate::keys::Cipher::None => name.starts_with(PLAIN_PREFIX),
        }
    }
}

#[derive(Debug)]
pub enum SetupError {
    Backend(BackendError),
    /// No part is stored under the name the record's digest maps to.
    Missing(PartRef),
    /// The stored part failed to authenticate, or its bytes don't match the record's digest.
    Corrupt(PartRef),
    /// A stored part, found by listing the store, failed to authenticate or is stored under a
    /// name its digest doesn't give. It carries the stored name.
    CorruptStored(String),
}

impl From<BackendError> for SetupError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

/// The SHA-256 a part is named by in records.
pub fn digest(bytes: &[u8]) -> Digest {
    Digest(Sha256::digest(bytes).into())
}

fn aad(name: &str) -> Vec<u8> {
    let mut aad = AAD_CONTEXT.to_vec();
    aad.extend_from_slice(name.as_bytes());
    aad
}

/// Reads and writes the deployment's setup parts, through a cache shared by every session in
/// the process: most runs name the same few parts.
pub struct SetupStore<'a, B: SessionBackend> {
    backend: &'a B,
    key: &'a SetupKey,
    cache: Mutex<HashMap<Digest, Arc<[u8]>>>,
}

impl<'a, B: SessionBackend> SetupStore<'a, B> {
    pub fn new(backend: &'a B, key: &'a SetupKey) -> Self {
        Self {
            backend,
            key,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn backend(&self) -> &'a B {
        self.backend
    }

    /// Stores `bytes` as a part of `kind`, durable on return, and returns the reference a record
    /// names it by, and whether this call asked the backend to store it. A part this store
    /// already read or stored is known to be stored, so it asks nothing.
    pub async fn put(
        &self,
        kind: crate::record::PartKind,
        bytes: &[u8],
    ) -> Result<(PartRef, bool), SetupError> {
        let part = PartRef {
            kind,
            digest: digest(bytes),
        };
        if self.cached(&part.digest).is_some() {
            return Ok((part, false));
        }
        let name = self.key.name(&part.digest);
        let sealed = self.key.cipher.seal_unit(&aad(&name), bytes);
        self.backend.put_setup(&name, &sealed).await?;
        self.remember(part.digest, bytes.into());
        Ok((part, true))
    }

    /// The bytes of the part with `digest`, checked against it, or `None` when no such part is
    /// stored: [`Self::get`] for a reader that knows the digest and not the kind of part it
    /// names. A part that fails its check is reported under its stored name.
    pub async fn read(&self, digest: &Digest) -> Result<Option<Arc<[u8]>>, SetupError> {
        // The kind only names the part in `get`'s errors, which are mapped here.
        let part = PartRef {
            kind: crate::record::PartKind::Tools,
            digest: *digest,
        };
        match self.get(&part).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(SetupError::Missing(_)) => Ok(None),
            Err(SetupError::Corrupt(_)) => Err(SetupError::CorruptStored(self.key.name(digest))),
            Err(error) => Err(error),
        }
    }

    /// The bytes of `part`, checked against its digest.
    pub async fn get(&self, part: &PartRef) -> Result<Arc<[u8]>, SetupError> {
        if let Some(bytes) = self.cached(&part.digest) {
            return Ok(bytes);
        }
        let name = self.key.name(&part.digest);
        let sealed = self
            .backend
            .get_setup(&name)
            .await?
            .ok_or_else(|| SetupError::Missing(part.clone()))?;
        let bytes = self
            .key
            .cipher
            .open_unit(&aad(&name), &sealed)
            .filter(|bytes| digest(bytes) == part.digest)
            .ok_or_else(|| SetupError::Corrupt(part.clone()))?;
        let bytes: Arc<[u8]> = bytes.into();
        self.remember(part.digest, bytes.clone());
        Ok(bytes)
    }

    /// Every part the deployment's setup store holds in this store's name space, with its
    /// digest. Each is read and checked as `get` checks it, and its stored name must be the one
    /// its digest gives, so a part copied under another name is damage, not a part. Parts of the
    /// other mode's name space are not this store's and are skipped unread.
    pub async fn load_all(&self) -> Result<Vec<(Digest, Arc<[u8]>)>, SetupError> {
        let mut parts = Vec::new();
        for name in self.backend.list_setup().await? {
            if !self.key.owns(&name) {
                continue;
            }
            let corrupt = || SetupError::CorruptStored(name.clone());
            let sealed = self.backend.get_setup(&name).await?.ok_or_else(corrupt)?;
            let bytes = self
                .key
                .cipher
                .open_unit(&aad(&name), &sealed)
                .ok_or_else(corrupt)?;
            let part_digest = digest(&bytes);
            if self.key.name(&part_digest) != name {
                return Err(corrupt());
            }
            let bytes: Arc<[u8]> = bytes.into();
            self.remember(part_digest, bytes.clone());
            parts.push((part_digest, bytes));
        }
        Ok(parts)
    }

    fn cached(&self, digest: &Digest) -> Option<Arc<[u8]>> {
        self.cache.lock().unwrap().get(digest).cloned()
    }

    fn remember(&self, digest: Digest, bytes: Arc<[u8]>) {
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= CACHE_CAP {
            cache.clear();
        }
        cache.insert(digest, bytes);
    }
}

#[cfg(test)]
mod tests;
