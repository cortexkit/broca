//! Where a render reads the bytes of the media a session captured, and how a read fails.
//!
//! References are never a license to trust cached or previously read bytes: every read returns
//! only bytes that match the reference's digest and size.

use broca_protocol::MediaRef;
use std::io;

/// Where a render reads the bytes of a medium its session captured: the session's own media
/// objects in the session store. Every source returns only bytes that match the reference's
/// digest and size, and refuses with the same [`MediaReadError`] codes, so the render rules in
/// `media_request` stay one set. A source is unwind safe: a read leaves nothing half changed.
pub trait MediaBytes: std::fmt::Debug + Send + Sync + std::panic::RefUnwindSafe {
    /// The medium's bytes, read afresh and checked against `reference`.
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError>;
}

impl<T: MediaBytes + ?Sized> MediaBytes for std::sync::Arc<T> {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        (**self).read_verified(reference)
    }
}

#[derive(Debug)]
pub enum MediaReadError {
    Missing(String),
    Corrupt(String),
    Io { digest: String, source: io::Error },
}

impl MediaReadError {
    /// Missing and corrupt objects have media refusal codes. Other I/O failures
    /// retain their source error for the caller's ordinary storage-error path.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Missing(_) => Some("media_object_missing"),
            Self::Corrupt(_) => Some("media_object_corrupt"),
            Self::Io { .. } => None,
        }
    }

    pub fn digest(&self) -> &str {
        match self {
            Self::Missing(digest) | Self::Corrupt(digest) | Self::Io { digest, .. } => digest,
        }
    }
}

impl std::fmt::Display for MediaReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            self.code().unwrap_or("read media object"),
            self.digest()
        )?;
        if let Self::Io { source, .. } = self {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MediaReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
