//! What the spend-fact builder reads of a session: one entry per record that changes a run's
//! facts, in the session's seq order.
//!
//! Each variant carries the fields the builder's rules read and nothing else. The host builds
//! them from the session's own records, so the builder doesn't depend on how the session is
//! stored.

use broca_protocol::{
    caller::RunFinishReason, usage::UsageIteration, BindIdentity, RunId, SessionScope, StepId,
    Usage,
};
use broca_records::record::{AttemptEnvelope, AuthSelection, RunConfig, StepFallback};

/// One record the builder reads, at its seq in the session. A warm fact's segment ordinal and
/// fact id are made from its seq, so seqs must not repeat within a session.
#[derive(Debug, Clone, PartialEq)]
pub struct BillingRecord {
    pub seq: u64,
    pub event: BillingEvent,
}

/// The records the builder reads, by what they change.
#[derive(Debug, Clone, PartialEq)]
pub enum BillingEvent {
    /// A run started (or resumed after a pause) under `config`. A start closes the run before
    /// it.
    RunStarted {
        run_id: RunId,
        session: BindIdentity,
        config: Box<RunConfig>,
        ts_ms: Option<u64>,
        scope: Option<SessionScope>,
    },
    /// The running run's render config was frozen again: its later steps bill in a new segment.
    ConfigRefrozen {
        renderer_version: u32,
        ts_ms: Option<u64>,
        frozen_render_config: serde_json::Value,
    },
    /// The running run moved to another account: its later steps bill in a new segment.
    AuthRefrozen {
        ts_ms: Option<u64>,
        auth_selection: AuthSelection,
    },
    /// A step finished with `usage`, served as `fallback` says.
    StepFinished {
        step_id: StepId,
        written_at_ms: Option<u64>,
        usage: Usage,
        retries_used: u32,
        service_tier_used: Option<String>,
        speed_used: Option<String>,
        fallback: Box<StepFallback>,
    },
    /// A step the provider refused after billing it.
    StepRefused {
        step_id: StepId,
        ts_ms: Option<u64>,
        usage: Usage,
        served_model: Option<String>,
        usage_iterations: Option<Vec<UsageIteration>>,
    },
    /// The run ended. `retries_used` is the run's total.
    RunFinished {
        reason: RunFinishReason,
        ts_ms: Option<u64>,
        retries_used: u32,
    },
    /// The run paused; it may resume under the same run id.
    RunPaused { ts_ms: Option<u64> },
    /// A keep-warm request re-sent step `step_id` of `run_id`.
    Warmed {
        run_id: RunId,
        step_id: StepId,
        usage: Usage,
        sent_at_ms: u64,
        envelope: Option<Box<AttemptEnvelope>>,
        trigger: Option<String>,
    },
}

impl BillingEvent {
    /// The run this record starts, if it is a start.
    pub fn started_run(&self) -> Option<&RunId> {
        match self {
            BillingEvent::RunStarted { run_id, .. } => Some(run_id),
            _ => None,
        }
    }
}
