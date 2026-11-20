//! Indexed reads use a bounded native page and only the unprojected WAL suffix.

/// Distinguishes an incomplete compaction projection from ordinary store or WAL faults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// Entry envelopes or their selection indexes have not been materialized yet.
    ProjectionNotReady,
    /// A store or WAL fault, retaining its diagnostic text.
    Failure(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProjectionNotReady => f.write_str("compaction entry projection is not ready"),
            Self::Failure(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ReadError {}

impl From<String> for ReadError {
    fn from(message: String) -> Self {
        Self::Failure(message)
    }
}

impl From<&str> for ReadError {
    fn from(message: &str) -> Self {
        Self::Failure(message.into())
    }
}
