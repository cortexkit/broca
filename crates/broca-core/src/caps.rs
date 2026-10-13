//! Durable budget-cap contracts shared by the receiver, coordinator, and store.
//!
//! The receiver accepts already-priced spend deltas. It never derives a price from
//! usage; the only arithmetic it performs is adding the supplied delta to a cap row.

use std::path::Path;

use serde::{Deserialize, Serialize};

/// The complete export-stream position applied to a cap row.
///
/// NEVER RECEIVED IN PRODUCTION. No `spend.delta` or `cap.install` has ever arrived: caps are
/// off, `cap_rows` holds one inactive tombstone from a probe, and nothing else. So this shape
/// is what I believe astrocyte sends, and their emitter is built against what they believe I
/// accept -- two independent readings of a contract neither side has ever exercised. They
/// agree today only because both were re-derived from this file within the same hour, after
/// their emitter was found sending `export_seq` while this field has always been
/// `consumed_export_seq`.
///
/// So agreement between the two sides is evidence about our reading process, not about the
/// wire. What would settle it is one real exchange in which either party could have refused.
/// Until a cap is armed, do not treat a passing test near this type as receiver acceptance:
/// every such test encodes and decodes my own struct against itself.
///
/// The `consumed_export_seq` / `seq` split against [`AppliedCapWatermark`] is deliberate and
/// load-bearing -- see that type. Do not add a serde alias to accept the other spelling: a
/// lenient decoder turns a malformed payload into a silent success, and under fail-closed cap
/// accounting the failure worth having is the one that names itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapWatermark {
    pub source_instance_id: String,
    pub source_epoch: u64,
    pub consumed_export_seq: u64,
}

impl CapWatermark {
    pub fn identity(&self) -> (&str, u64) {
        (&self.source_instance_id, self.source_epoch)
    }
}

/// A watermark echoed after a durable mutation. The wire contract calls the sequence
/// field `seq` to distinguish the acknowledgement from an input delta watermark.
///
/// The asymmetry with [`CapWatermark`]'s `consumed_export_seq` is intentional: a request names
/// the position the sender CONSUMED, an acknowledgement names the position I durably APPLIED.
/// They are the same triple making different claims, so confusing the two cannot silently
/// typecheck. It reads as an oversight and it is not -- astrocyte's design document documented
/// THIS struct in the request slot for weeks, which is worse than a typo because checking that
/// line against this file confirms it. Renaming either field to match the other would remove
/// the only thing that catches the confusion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedCapWatermark {
    pub source_instance_id: String,
    pub source_epoch: u64,
    pub seq: u64,
}

impl From<&CapWatermark> for AppliedCapWatermark {
    fn from(value: &CapWatermark) -> Self {
        Self {
            source_instance_id: value.source_instance_id.clone(),
            source_epoch: value.source_epoch,
            seq: value.consumed_export_seq,
        }
    }
}

/// The baseline that installs the complete durable state of a cap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapBaseline {
    pub spend_nanos: i64,
    pub unresolved_count: i64,
    pub watermark: CapWatermark,
    /// The period instance currently represented by the baseline. Older clients may omit it;
    /// the first valid spend delta then anchors the row to its bucket.
    #[serde(default)]
    pub period_bucket: Option<String>,
    pub as_of_ms: i64,
}

/// Whether an install request adds/replaces a cap or retires one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapOperation {
    Install,
    Remove,
}

/// A daemon-scoped cap-table mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapInstallRequest {
    pub cap_id: String,
    pub cap_version: u64,
    pub op: CapOperation,
    pub scope_key: String,
    pub project_root: Option<String>,
    pub constraint_dim: String,
    pub period: String,
    pub period_tz: String,
    pub hard_cap_nanos: i64,
    pub max_staleness_ms: u64,
    pub replaces_cap_id: Option<String>,
    pub baseline: Option<CapBaseline>,
}

/// An already-priced spend increment for exactly one cap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpendDeltaRequest {
    pub cap_id: String,
    pub cap_version: u64,
    pub from_watermark: CapWatermark,
    pub to_watermark: CapWatermark,
    pub priced_delta_nanos: i64,
    pub unresolved_delta: i64,
    pub period_bucket: String,
}

/// The state kept in the in-memory admission mirror and in durable cap rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapRow {
    pub cap_id: String,
    pub cap_version: u64,
    pub scope_key: String,
    pub project_root: Option<String>,
    pub constraint_dim: String,
    pub period: String,
    pub period_tz: String,
    pub hard_cap_nanos: i64,
    pub max_staleness_ms: u64,
    pub replaces_cap_id: Option<String>,
    /// The period instance accumulated by this row; absent only for legacy or compatibility
    /// baselines until the first valid spend delta anchors the row.
    pub period_bucket: Option<String>,
    pub spend_nanos: i64,
    pub unresolved_count: i64,
    pub applied_watermark: CapWatermark,
    pub as_of_ms: i64,
    pub active: bool,
    /// A replacement tombstone keeps the retired row so a late remove of the old
    /// cap cannot undo the replacement transaction.
    pub replaced_by: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapMutation {
    Applied(AppliedCapWatermark),
    BaselineRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapStoreError {
    BaselineRequired,
    Storage(String),
    Invalid(String),
}

impl std::fmt::Display for CapStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BaselineRequired => f.write_str("baseline_required"),
            Self::Storage(message) => write!(f, "cap storage error: {message}"),
            Self::Invalid(message) => write!(f, "invalid cap request: {message}"),
        }
    }
}

impl std::error::Error for CapStoreError {}

/// The synchronous seam used by the actor's zero-I/O admission path and by the
/// asynchronous management-surface receiver.
pub trait CapStore: Send + Sync {
    fn install_cap(&self, request: &CapInstallRequest) -> Result<CapMutation, CapStoreError>;
    fn apply_spend_delta(&self, request: &SpendDeltaRequest) -> Result<CapMutation, CapStoreError>;
    fn matching_caps(&self, scope_key: &str, project_root: &Path) -> Vec<CapRow>;
}
