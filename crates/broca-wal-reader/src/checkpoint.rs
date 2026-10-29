//! Versioned billing state bound to the complete boundary frame, not a bare watermark.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) const VERSION: u64 = 2;
// chatgpt-codex-wire/v1 adds transport digests outside the reservation and envelope returned
// by Billed. Frame stamps and writer succession are interpreted by the positioned reader and
// billing fold respectively.
pub const KNOWN_FEATURES: &[&str] = &[
    "bill-attempts/v1",
    "chatgpt-codex-wire/v1",
    "frame-stamps/v1",
    "writer-id/v1",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Restart {
    StateVersion,
    FeatureTable,
    Lineage,
    BoundaryMissing,
    BoundaryLength,
    BoundaryHash,
    BoundarySequence,
    SequenceGap,
    InterveningLineage,
    TailBoundary,
    FoldedState,
    ReservationReopened,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Boundary {
    pub seq: u64,
    pub offset: u64,
    pub len: u64,
    pub hash: [u8; 32],
}

/// Persist only after consuming every item and committing unreadable frames plus a record of
/// which billing evidence could not be determined.
/// The serialized state is trusted local reader state, not an authenticated receipt.
#[derive(Debug, Clone, PartialEq)]
pub struct Checkpoint {
    pub(crate) version: u64,
    pub(crate) lineage: [u8; 16],
    pub(crate) boundary: Option<Boundary>,
    pub(crate) features: [u8; 32],
    pub(crate) state: Value,
}

impl Checkpoint {
    pub fn seq(&self) -> u64 {
        self.boundary.as_ref().map_or(0, |b| b.seq)
    }

    pub fn lineage_id(&self) -> [u8; 16] {
        self.lineage
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let boundary = self
            .boundary
            .as_ref()
            .map(|b| json!({"seq": b.seq, "offset": b.offset, "len": b.len, "sha256": b.hash}));
        serde_json::to_vec(&json!({
            "version": self.version, "lineage": self.lineage, "boundary": boundary,
            "features": self.features, "state": self.state
        }))
        .expect("checkpoint consists only of JSON values")
    }

    /// Malformed bytes are a named restart, not permission to trust a partial checkpoint.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Restart> {
        let v: Value = serde_json::from_slice(bytes).map_err(|_| Restart::FoldedState)?;
        let boundary = if v["boundary"].is_null() {
            None
        } else {
            let b = &v["boundary"];
            Some(Boundary {
                seq: b["seq"].as_u64().ok_or(Restart::FoldedState)?,
                offset: b["offset"].as_u64().ok_or(Restart::FoldedState)?,
                len: b["len"].as_u64().ok_or(Restart::FoldedState)?,
                hash: array(&b["sha256"]).ok_or(Restart::FoldedState)?,
            })
        };
        Ok(Self {
            version: v["version"].as_u64().ok_or(Restart::StateVersion)?,
            lineage: array(&v["lineage"]).ok_or(Restart::FoldedState)?,
            boundary,
            features: array(&v["features"]).ok_or(Restart::FeatureTable)?,
            state: v["state"].clone(),
        })
    }
}

pub(crate) fn feature_digest() -> [u8; 32] {
    Sha256::digest(KNOWN_FEATURES.join("\n").as_bytes()).into()
}

pub(crate) fn array<const N: usize>(v: &Value) -> Option<[u8; N]> {
    let values = v.as_array()?;
    if values.len() != N {
        return None;
    }
    let mut out = [0; N];
    for (byte, value) in out.iter_mut().zip(values) {
        *byte = u8::try_from(value.as_u64()?).ok()?;
    }
    Some(out)
}
