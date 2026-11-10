//! Byte-preserving exports of a pinned session. A move holds the session lease; no writer,
//! collector or restore may change the graph until the move releases or aborts its fence.

use std::{collections::BTreeMap, path::Path};

use sha2::{Digest as _, Sha256};

use crate::{
    backend::{BackendError, LocalBackend, SessionAddr, SessionBackend, Version},
    codec::{hex, Message as _},
    head::{self, Head, MoveResidence, Residence, SessionTriple},
    keys::{Cipher, DeploymentKey},
};

pub const FORMAT: &str = "broca-store-export/1";
const MAGIC: &[u8] = b"broca-store-export/1\0";

crate::message! {
    /// Catalog information needed before a receiver writes any files.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Header {
        1 req triple: SessionTriple,
        2 req class: String,
        3 req cipher: Cipher,
        4 opt deployment_key: u16,
    }
}

#[derive(Debug)]
pub enum Error {
    Backend(BackendError),
    Invalid(String),
    DeploymentKeyMissing(u16),
}

impl From<BackendError> for Error {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Backend(error.into())
    }
}
fn invalid(reason: impl std::fmt::Debug) -> Error {
    Error::Invalid(format!("{reason:?}"))
}

/// A fully checked framing; the values are stored bytes, never decrypted payloads.
pub struct Export {
    pub header: Header,
    pub files: BTreeMap<String, Vec<u8>>,
}

impl Export {
    /// Walks exactly what `version` reaches, including setup, media and import objects. The
    /// frozen version is kept by the backend, so retries rebuild identical bytes without an
    /// extra copy of the stream on disk.
    pub async fn pinned(
        store: &LocalBackend,
        addr: &SessionAddr,
        version: Version,
        class: &str,
        deployment: &DeploymentKey,
    ) -> Result<Self, Error> {
        let bytes = store
            .read_head_version(addr, version)?
            .ok_or_else(|| invalid("pinned head is missing"))?;
        let (head, _) = head::open(&bytes, deployment).map_err(invalid)?;
        if !matches!(head.residence, Residence::Local(())) {
            return Err(invalid("export head is not local"));
        }
        let (mut files, finding) =
            super::restore::reachable(store, addr, deployment, &bytes, version)
                .await
                .map_err(invalid)?;
        if let Some(finding) = finding {
            return Err(invalid(finding));
        }
        let mut stored = version.to_le_bytes().to_vec();
        stored.extend_from_slice(&bytes);
        files.insert(format!("heads/{}/head", addr.0), stored.clone());
        files.insert(format!("heads/{}/v/{version}", addr.0), stored);
        let header = Header {
            triple: head.triple,
            class: class.into(),
            cipher: head.cipher.unwrap_or(Cipher::XChaCha20Poly1305),
            deployment_key: head.keys.first().map(|key| key.deployment),
        };
        let export = Self { header, files };
        export.check(addr, deployment)?;
        Ok(export)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = MAGIC.to_vec();
        put_string(&mut out, &self.header.encode());
        out.extend_from_slice(&(self.files.len() as u32).to_le_bytes());
        for (path, bytes) in &self.files {
            put_string(&mut out, path.as_bytes());
            out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            out.extend_from_slice(&Sha256::digest(bytes));
            out.extend_from_slice(bytes);
        }
        out
    }

    /// Checks every file's SHA before staging and requires the receiver's key before any
    /// file is written. Bounds come from the actual input slice, never declared lengths.
    pub fn decode(
        mut bytes: &[u8],
        addr: &SessionAddr,
        deployment: &DeploymentKey,
    ) -> Result<Self, Error> {
        if take(&mut bytes, MAGIC.len())? != MAGIC {
            return Err(invalid("export format"));
        }
        let header = Header::decode(string(&mut bytes)?).map_err(invalid)?;
        preflight(&header, deployment)?;
        let count = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap());
        let mut files = BTreeMap::new();
        for _ in 0..count {
            let path = std::str::from_utf8(string(&mut bytes)?)
                .map_err(invalid)?
                .to_owned();
            let len = u64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
            let expected = take(&mut bytes, 32)?;
            let data = take(&mut bytes, usize::try_from(len).map_err(invalid)?)?;
            if Sha256::digest(data).as_slice() != expected {
                return Err(invalid(format!("SHA-256 mismatch: {path}")));
            }
            if files.insert(path, data.to_vec()).is_some() {
                return Err(invalid("duplicate export path"));
            }
        }
        if !bytes.is_empty() {
            return Err(invalid("trailing export bytes"));
        }
        let export = Self { header, files };
        export.check(addr, deployment)?;
        Ok(export)
    }

    pub fn head(
        &self,
        addr: &SessionAddr,
        deployment: &DeploymentKey,
    ) -> Result<(Head, Version), Error> {
        let stored = self
            .files
            .get(&format!("heads/{}/head", addr.0))
            .ok_or_else(|| invalid("missing export head"))?;
        let mut bytes = stored.as_slice();
        let version = u64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
        let (head, _) = head::open(bytes, deployment).map_err(invalid)?;
        Ok((head, version))
    }

    fn check(&self, addr: &SessionAddr, deployment: &DeploymentKey) -> Result<(), Error> {
        preflight(&self.header, deployment)?;
        if !matches!(
            self.header.class.as_str(),
            "durable" | "ordinary" | "ephemeral"
        ) {
            return Err(invalid("unknown storage class"));
        }
        let (head, version) = self.head(addr, deployment)?;
        if head.triple != self.header.triple
            || head.cipher() != self.header.cipher
            || !matches!(head.residence, Residence::Local(()))
        {
            return Err(invalid("export header differs from local head"));
        }
        let current = format!("heads/{}/head", addr.0);
        let pinned = format!("heads/{}/v/{version}", addr.0);
        if self.files.get(&pinned) != self.files.get(&current) {
            return Err(invalid("pinned head differs"));
        }
        for path in self.files.keys() {
            let safe = path.split('/').all(|part| {
                !part.is_empty()
                    && part != "."
                    && part != ".."
                    && part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            });
            let owned = path == &current
                || path == &pinned
                || path.starts_with(&format!("objects/{}/", addr.0))
                || path.starts_with(&format!("logs/{}/", addr.0))
                || path.starts_with("setup/")
                || path.starts_with("setup-plain/");
            if !safe || !owned {
                return Err(invalid(format!("export path outside session: {path}")));
            }
        }
        Ok(())
    }
}

fn preflight(header: &Header, deployment: &DeploymentKey) -> Result<(), Error> {
    match (header.cipher, header.deployment_key) {
        (Cipher::None, None) => Ok(()),
        (Cipher::XChaCha20Poly1305, Some(id))
            if deployment.is_loaded() && deployment.id() == id =>
        {
            Ok(())
        }
        (Cipher::XChaCha20Poly1305, Some(id)) => Err(Error::DeploymentKeyMissing(id)),
        _ => Err(invalid("cipher and deployment key disagree")),
    }
}
fn put_string(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}
fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if len > bytes.len() {
        return Err(invalid("truncated export"));
    }
    let (part, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(part)
}
fn string<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], Error> {
    let len = u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap());
    take(bytes, len as usize)
}

mod files;
pub use files::{abort, freeze, release};

#[cfg(test)]
mod tests;
