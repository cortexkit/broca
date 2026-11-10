//! Recovery evidence embedded in durable records.

use broca_protocol::{BatchId, RunId, StepId, ToolCallId};
use serde::{Deserialize, Serialize};

pub const FEATURE: &str = "wal-recovery/v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutReason {
    UnpublishedTail,
    PublishedRegionCorruption,
    UnprovenCorruption,
}

impl CutReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnpublishedTail => "unpublished_tail",
            Self::PublishedRegionCorruption => "published_region_corruption",
            Self::UnprovenCorruption => "unproven_corruption",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCall {
    pub tool_call_id: ToolCallId,
    pub step_id: Option<StepId>,
    pub batch_id: Option<BatchId>,
    /// Exact Prepared/intent keys recovered from the record or its frame identity.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_keys: Vec<String>,
}

impl RecoveryCall {
    /// Whether evidence identifies more than the model-facing ID alone.
    pub fn has_effect_identity(&self) -> bool {
        self.batch_id.is_some() || self.step_id.is_some() || !self.call_keys.is_empty()
    }

    /// Within an already matched owner, compare effect identity rather than a
    /// run-wide model-facing ID. Unknown
    /// batch/step identities without an exact key do not even match each other.
    pub fn matches_effect(&self, other: &Self) -> bool {
        if self.tool_call_id != other.tool_call_id
            || self
                .batch_id
                .as_ref()
                .zip(other.batch_id.as_ref())
                .is_some_and(|(a, b)| a != b)
            || self.step_id.zip(other.step_id).is_some_and(|(a, b)| a != b)
        {
            return false;
        }
        let shared_key = self
            .call_keys
            .iter()
            .any(|key| other.call_keys.contains(key));
        if !self.call_keys.is_empty() && !other.call_keys.is_empty() && !shared_key {
            return false;
        }
        shared_key
            || self
                .batch_id
                .as_ref()
                .zip(other.batch_id.as_ref())
                .is_some_and(|(a, b)| a == b)
            || self.step_id.zip(other.step_id).is_some_and(|(a, b)| a == b)
    }

    /// A known physical key requires a terminal identifying that exact effect;
    /// a same-ID result from another step is never sufficient evidence.
    pub fn terminal_matches(&self, terminal: &Self) -> bool {
        self.matches_effect(terminal)
            && (self.call_keys.is_empty()
                || self
                    .call_keys
                    .iter()
                    .any(|key| terminal.call_keys.contains(key)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryOutcome {
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnownedRecoveryCall {
    pub outcome: RecoveryOutcome,
    /// Evidence only: a model-facing ID does not identify a later run's call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<ToolCallId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<StepId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<BatchId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_keys: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRun {
    pub run_id: RunId,
    pub calls: Vec<RecoveryCall>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryCut {
    pub cut_id: String,
    pub sidecar_basename: String,
    pub cut_offset: u64,
    pub removed_len: u64,
    pub removed_sha256: String,
    pub reason: CutReason,
    pub active_run: Option<RunId>,
    pub runs: Vec<RecoveryRun>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unowned_calls: Vec<UnownedRecoveryCall>,
    pub unattributed_tool_risk: bool,
}
