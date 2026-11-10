//! Durable compaction metadata. These are storage shapes, not a second implementation of
//! the provider's scheduling or fence rules; those belong to the pinned role crate.

use broca_protocol::content::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Half the maximum session-read page size, and well below the WAL frame limit.
pub const MAX_REPLACEMENT_BYTES: usize = 8 * 1024 * 1024;

/// A half-open range of the original transcript's ordinals; an empty range is an insertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionRange {
    pub from: u64,
    pub to: u64,
}

/// The provider's durable cursor is scoped to a lineage. `None` means no ordinal consumed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionCursor {
    pub lineage_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ordinal: Option<u64>,
}

/// Setup's ranks key on positions in every replacement, not transcript ordinals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionStabilityRank {
    pub index: u32,
    pub rank: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionMessageKind {
    Setup,
    Step,
}

/// Every step outcome other than an applied replacement. Closed because a reader must
/// understand a disposition before a writer may put it under `compaction/v1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionDisposition {
    Noop,
    Wait,
    Refuse,
    SupersededRequest,
    Late,
    RangeInverted,
    StaleVersion,
    RangeBeyondNewest,
    Structural,
    FailedCall,
    Timeout,
}

/// A rejected replacement's identity, without its possibly oversized content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedCompaction {
    pub compaction_id: String,
    pub version: u64,
    pub range: CompactionRange,
}

/// Written atomically with a wait answer so a crash cannot lose the open wait's bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionWait {
    pub reason: String,
    pub bound_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionWaitExit {
    Ready,
    Bound,
    Cap,
    Exceeded,
    Cancel,
    Stopped,
    Interrupted,
}

/// A half-open range of transcript ordinals, `[from_ordinal, to_ordinal)`, in which every
/// signed reasoning block of a message the request sends as written is left out of the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionStrip {
    pub from_ordinal: u64,
    pub to_ordinal: u64,
}

/// How an asynchronous compaction view went into effect (compaction-provider/v1 §7).
///
/// Closed: a reader must understand a choice before it can render the request the writer
/// rendered, and the record that carries it is must-understand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompactionViewChoice {
    /// Applied as published: no signed reasoning the request keeps sat after the view's first
    /// changed byte.
    Applied,
    /// Applied with the companion edit `edit` (today only `strip_signed_reasoning`): signed
    /// reasoning produced against the prefix the view changed is left out of every request
    /// from now on, because the model provider rejects a signature whose prefix changed.
    Companion {
        edit: String,
        strip: CompactionStrip,
    },
}

/// SHA-256 of the compact JSON encoding of the native replacement message array, as
/// `serde_json::to_vec` writes it (no newline). Metadata and the outer frame are excluded.
/// Readers and the later writer use this domain regardless of the provider's JSON layout.
pub fn compaction_replacement_sha256(replacement: &[Message]) -> Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(replacement)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
