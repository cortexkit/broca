use std::fmt;

/// A header cannot establish a trustworthy next-frame boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeaderCorruption {
    CrcMismatch,
    PayloadTooLarge { len: u32 },
    InvalidRequires,
    InvalidFeatureName { name: String },
}

/// Codec failures. Unlike a bad payload, these do not permit skipping a frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnsupportedVersion {
        ver: u8,
    },
    HeaderCorrupt {
        reason: HeaderCorruption,
    },
    /// A log requiring these unknown features cannot be read by this reader.
    NewerWriter {
        features: Vec<String>,
    },
    PayloadTooLarge {
        len: usize,
    },
    RequiresTooLarge {
        len: usize,
    },
    InvalidFeatureName {
        name: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { ver } => write!(f, "unsupported WAL framing version {ver}"),
            Self::HeaderCorrupt { reason } => write!(f, "version 4 WAL header corrupt: {reason:?}"),
            Self::NewerWriter { features } => {
                write!(f, "WAL requires unknown features: {}", features.join(", "))
            }
            Self::PayloadTooLarge { len } => {
                write!(f, "WAL payload of {len} bytes exceeds the cap")
            }
            Self::RequiresTooLarge { len } => {
                write!(
                    f,
                    "WAL requires array of {len} bytes exceeds the u16 header limit"
                )
            }
            Self::InvalidFeatureName { name } => write!(
                f,
                "invalid WAL feature name {name:?} (expected [a-z0-9-]+/v<digits>)"
            ),
        }
    }
}

impl std::error::Error for Error {}
