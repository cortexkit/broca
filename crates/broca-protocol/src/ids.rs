//! Identifier newtypes that cross the wire. Kept as thin transparent wrappers
//! so the protocol crate stays free of id-generation policy (the runner assigns
//! them); these only pin the wire shapes.

use serde::{Deserialize, Serialize};

/// Unique id for one run (one `start_run` invocation). Caller- or runner-assigned.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub String);

/// Unique id for one submission — a single prompt injection into a session. A
/// submission is durably queued, can be retracted, and is consumed into exactly one
/// run when the coordinator starts it. Carried on the `RunStarted` that consumes it so
/// replay can tell which queued submission became which run (and never double-consume
/// one across a crash).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubmissionId(pub String);

/// Monotonic step index within a run — one model round-trip = one step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StepId(pub u64);

/// Provider-assigned id for one tool call. Used as the dedup/fence key so a
/// repeated call is answered from the prior result instead of re-executed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolCallId(pub String);

/// Id for one tool BATCH — the set of tool calls in one assistant turn. The WAL
/// brackets a batch with start/finish markers, and resume only reconstructs the
/// prompt from batches that finished — so a half-written batch is never replayed
/// as a (provider-invalid) partial turn.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BatchId(pub String);

/// Id for one assembled assistant message / content-block stream. Links a
/// streamed display delta back to its authoritative assembled message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(pub String);

/// Per-run provider-stream generation counter. Carried in the transport envelope's
/// `channel` field on the direct socket so frames from a superseded stream are
/// detectable WITHOUT parsing the body. `u16` matches the envelope field; it is a
/// wrapping tag — the runner only compares it against the current expected
/// generation to abandon in-flight assembly, never as a globally-unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamGen(pub u16);
