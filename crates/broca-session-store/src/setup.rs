//! The setup store: the parts a run is started with, stored once per deployment and shared by
//! every session.
//!
//! A run's setup is everything it is frozen with except the conversation: the tools offered to
//! the model, each tool provider's catalog answer, the system text, the plan, and so on. Each is
//! a **part**, named in records by the plain SHA-256 of its canonical bytes (`PartRef`). Almost
//! every part is shared by many sessions (many sessions of one project are started with the same tools), so each
//! is stored once, not in every run start.
//!
//! Storage:
//! - **Name:** a part is stored under an HMAC of its digest with the deployment's naming key. A
//!   record's plain digest stays inside encrypted records, and a stored name reveals nothing about
//!   the content to whoever holds the storage. A session moved to another deployment finds its
//!   parts there under that deployment's names.
//! - **Encryption:** each part is encrypted under the deployment's setup key, with its name in
//!   the authenticated data, so a part served under another name fails to open.
//! - **Verification:** a read checks the decrypted bytes against the digest the record names. A
//!   missing or corrupt part is damage to the session that names it, never an absent value.
//! - **Lifetime:** parts are never deleted: they amount to a few megabytes a month, and deleting
//!   one safely would need every session's head checked first. Storing a part that is already
//!   there changes nothing.
//!
//! A run start that names a part is written only after the part's store returned durable, so a
//! crash can never leave a record naming a part that isn't there.
//!
//! Parts must never carry session content: the per-session fields of a run's setup (its account,
//! routing decisions, prompt cache key) stay in the run's own record.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sha2::{Digest as _, Sha256};

use crate::backend::{BackendError, SessionBackend};
use crate::codec::{hex, Digest};
use crate::frame::FrameKey;
use crate::record::PartRef;

const AAD_CONTEXT: &[u8] = b"broca-setup-part-v1";

/// Parts the in-memory cache holds before it starts over. The deployment's distinct parts number
/// in the hundreds, so this only bounds a cache that would otherwise grow without limit.
const CACHE_CAP: usize = 4096;

/// The deployment's setup-store keys: one to name parts, one to encrypt them.
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

    /// The name part `digest` is stored under.
    pub fn name(&self, digest: &Digest) -> String {
        hex(&crate::prune::keyed_digest(&self.naming, &digest.0).0)
    }
}

#[derive(Debug)]
pub enum SetupError {
    Backend(BackendError),
    /// No part is stored under the name the record's digest maps to.
    Missing(PartRef),
    /// The stored part failed to authenticate, or its bytes don't match the record's digest.
    Corrupt(PartRef),
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

    pub fn key(&self) -> &'a SetupKey {
        self.key
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
