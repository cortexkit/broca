//! Content-free retention markers, outside the heads, logs and objects deletion removes.
//!
//! A marker is published under the session lease before its content is deleted. It remains
//! after the address is reused, both to answer reads of deleted runs and to prevent tag reuse.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use broca_owner_only::OpenOptions;

use crate::backend::{sync_dir, SessionAddr};
use crate::codec::{hex, Id128, Message as _};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};

const FORMAT: u32 = 1;
// Bound decoding and allocation even when the stored file is damaged. Exceeding either limit
// refuses deletion rather than losing the ability to recognize an expired run.
pub const MAX_RUNS: usize = 1_048_576;
const MAX_BYTES: u64 = 64 * 1024 * 1024;

crate::message! {
    /// Version 1 of a deleted session's content-free expiry marker.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Marker {
        1 req format: u32,
        2 req triple: SessionTriple,
        3 req expired_at_ms: u64,
        4 req seq: u64,
        5 req next_tag: u64,
        6 req next_notice_seq: u64,
        7 rep runs: String,
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    KeyUnavailable,
    Damaged(String),
    TooLarge,
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn folder(root: &Path, addr: &SessionAddr) -> Result<PathBuf, Error> {
    if addr.0.is_empty() || !addr.0.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Damaged("expiry address is not hex".into()));
    }
    Ok(root.join("expired").join(&addr.0))
}

fn aad(addr: &SessionAddr, name: &str, prefix: &[u8]) -> Vec<u8> {
    let mut out = format!("broca-expired-v1/{}/{name}", addr.0).into_bytes();
    out.extend_from_slice(prefix);
    out
}

/// Reads every completed marker at the address, checking its mode, identity and filename.
/// A missing deployment key is never treated as an absent marker.
pub fn read(
    root: &Path,
    addr: &SessionAddr,
    triple: &SessionTriple,
    deployment: &DeploymentKey,
) -> Result<Vec<Marker>, Error> {
    let entries = match std::fs::read_dir(folder(root, addr)?) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut markers = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| Error::Damaged("expiry name".into()))?;
        if name.starts_with('.') {
            continue;
        }
        if name.len() != 32 || !name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(Error::Damaged("expiry name is not a session id".into()));
        }
        let mut bytes = Vec::new();
        std::fs::File::open(entry.path())?
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(Error::TooLarge);
        }
        let prefix = bytes
            .get(..3)
            .ok_or_else(|| Error::Damaged("expiry prefix".into()))?;
        let cipher = match prefix[0] {
            0 => Cipher::None,
            1 => Cipher::XChaCha20Poly1305,
            _ => return Err(Error::Damaged("expiry cipher".into())),
        };
        let key = deployment.expiry_key(cipher).ok_or(Error::KeyUnavailable)?;
        if cipher != Cipher::None && prefix[1..] != deployment.id().to_le_bytes() {
            return Err(Error::KeyUnavailable);
        }
        let body = key
            .open_unit(&aad(addr, name, prefix), &bytes[3..])
            .ok_or_else(|| Error::Damaged("expiry authentication".into()))?;
        let mut runs = 0;
        for field in crate::codec::Fields::new(&body, "expiry marker") {
            let field = field.map_err(|error| Error::Damaged(format!("{error:?}")))?;
            if field.number == 7 {
                runs += 1;
                if runs > MAX_RUNS {
                    return Err(Error::TooLarge);
                }
            }
        }
        let marker = Marker::decode(&body).map_err(|error| Error::Damaged(format!("{error:?}")))?;
        if marker.format != FORMAT || &marker.triple != triple {
            return Err(Error::Damaged("expiry version or session identity".into()));
        }
        if marker.runs.len() > MAX_RUNS {
            return Err(Error::TooLarge);
        }
        markers.push(marker);
    }
    Ok(markers)
}

/// Writes and syncs a marker before its session is deleted. The caller holds the lease.
/// Repeating a deletion keeps its original sequence and expiry time.
pub fn write(
    root: &Path,
    addr: &SessionAddr,
    session_id: &Id128,
    mut marker: Marker,
    cipher: Cipher,
    deployment: &DeploymentKey,
) -> Result<(), Error> {
    let previous = read(root, addr, &marker.triple, deployment)?;
    let dir = folder(root, addr)?;
    let name = hex(&session_id.0);
    if dir.join(&name).try_exists()? {
        return Ok(());
    }
    marker.format = FORMAT;
    marker.seq = previous
        .iter()
        .map(|marker| marker.seq)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(Error::TooLarge)?;
    marker.next_tag = marker
        .next_tag
        .max(previous.iter().map(|m| m.next_tag).max().unwrap_or(1));
    marker.next_notice_seq = marker.next_notice_seq.max(
        previous
            .iter()
            .map(|m| m.next_notice_seq)
            .max()
            .unwrap_or(1),
    );
    if marker.runs.len() > MAX_RUNS {
        return Err(Error::TooLarge);
    }
    let body = marker.encode();
    if body.len() as u64 > MAX_BYTES - 64 {
        return Err(Error::TooLarge);
    }
    let key = deployment.expiry_key(cipher).ok_or(Error::KeyUnavailable)?;
    let mut bytes = vec![u8::from(cipher != Cipher::None)];
    bytes.extend_from_slice(&deployment.id().to_le_bytes());
    let sealed = key.seal_unit(&aad(addr, &name, &bytes), &body);
    bytes.extend_from_slice(&sealed);
    broca_owner_only::create_dir_all(&dir)?;
    // Sync each newly introduced parent as well as the marker's own directory.
    sync_dir(root)?;
    sync_dir(&root.join("expired"))?;
    let temp = dir.join(format!(".{name}.tmp"));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temp, dir.join(name))?;
    sync_dir(&dir)?;
    Ok(())
}

#[cfg(test)]
mod tests;
