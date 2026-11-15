//! `session.request_dump`: the exact request body each step of one run sent, rebuilt from the
//! records of a session kept in the new storage, for the operator to read.
//!
//! The new storage keeps each attempt's request digest, not its bytes. This operation rebuilds
//! each step's request through the same path broca's warm-ups use, renders it with the run's
//! frozen render config, and writes it as the step's body only when its SHA-256 equals the digest
//! the step's last attempt recorded. A body that rebuilds to other bytes is written beside, under
//! a name that says so, for finding what differs. The bodies go to a private folder under broca's
//! state root; the reply names the folder and each step's outcome, never any content.
//!
//! Operator only, and outside the runner role.

use serde::{Deserialize, Serialize};

pub const OP_SESSION_REQUEST_DUMP: &str = "session.request_dump";

/// `session.request_dump` request parameters. An unknown field is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestDumpParams {
    /// The run whose steps are rebuilt.
    pub run_id: String,
}

/// The reply: where the bodies were written, and what happened to each step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestDumpResponse {
    pub run_id: String,
    /// The private folder holding the written bodies: `s<step>.json` per written step, and
    /// `s<step>.mismatch.json` per step that rebuilt to other bytes.
    pub folder: String,
    /// Every step the run attempted, in step order.
    pub steps: Vec<StepDump>,
}

/// One step of the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepDump {
    pub step: u64,
    /// The SHA-256 the step's last attempt recorded for the request it sent, in lowercase hex.
    pub recorded_sha256: String,
    pub outcome: StepDumpOutcome,
    /// The written body's length in bytes; present when `outcome` is `written` or
    /// `digest_mismatch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Why the step wasn't written as its body: the digest it rebuilt to and the length the step
    /// sent, or why it couldn't be rebuilt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepDumpOutcome {
    /// Rebuilt to the recorded digest and written.
    Written,
    /// Rebuilt, but to other bytes than the step sent; written as `s<step>.mismatch.json`.
    DigestMismatch,
    /// Couldn't be rebuilt (for example, a request carrying media); nothing was written.
    NotRebuilt,
}
