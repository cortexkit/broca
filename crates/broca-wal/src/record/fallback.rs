//! Storage shapes for content-filter fallback on Anthropic runs: the server-side fallback a
//! response reports (`provider-fallback-block/v1`) and the client-side refusal fallback the run
//! loop drives (`refusal-fallback/v1`). The schedule's transitions live in broca-core; these
//! are only the values the WAL records.

use broca_protocol::request::ModelHandle;
use serde::{Deserialize, Serialize};

use super::is_false;

/// The fallback facts a `ModelStepFinished` can carry, flattened into the record so each
/// member is an ordinary top-level key. Every member is absent by default and an absent member
/// writes no bytes, so a step without them keeps the bytes it had before these existed.
///
/// `served_model` and `usage_iterations` require `provider-fallback-block/v1` when present;
/// `route`, `refusal_fallback` and `refusal_after_tool_use` require `refusal-fallback/v1`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StepFallback {
    /// The wire model id that produced the committed answer, when a server-side fallback
    /// served it with a model other than the one requested. Absent when the requested model
    /// served it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_model: Option<String>,
    /// The response's `usage.iterations`, kept as reported, when the response carried them.
    /// The step's `usage` stays the provider's top-level usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_iterations: Option<Vec<UsageIteration>>,
    /// Present only when the step was sent to the client-side fallback model instead of the
    /// episode's model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<StepRoute>,
    /// The session's refusal-fallback schedule after this step. Written on every step while a
    /// schedule exists, and once more on the step that clears it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal_fallback: Option<RefusalFallbackSchedule>,
    /// The source model refused after completing at least one tool call; the step committed as
    /// a tool step and armed the schedule. Omitted when false.
    #[serde(default, skip_serializing_if = "is_false")]
    pub refusal_after_tool_use: bool,
}

impl StepFallback {
    /// Whether any member that requires `provider-fallback-block/v1` is present.
    pub fn carries_server_side_fallback(&self) -> bool {
        self.served_model.is_some() || self.usage_iterations.is_some()
    }

    /// Whether any member that requires `refusal-fallback/v1` is present.
    pub fn carries_refusal_fallback(&self) -> bool {
        self.route.is_some() || self.refusal_fallback.is_some() || self.refusal_after_tool_use
    }
}

/// One entry of an Anthropic response's `usage.iterations`, kept as reported so a later split of
/// one step's usage across the models that ran can be computed from the WAL alone.
pub use broca_protocol::usage::UsageIteration;

/// Which model a step was sent to when it was not the episode's own model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepRoute {
    /// The client-side fallback model that answered the step.
    pub model: ModelHandle,
    pub reason: StepRouteReason,
}

/// Why a step went to the client-side fallback model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepRouteReason {
    /// The source model refused this same step, and the step was re-sent.
    Refusal,
    /// An earlier refusal armed the schedule, and the schedule still routes to the fallback.
    Schedule,
}

/// A session's client-side refusal-fallback schedule, as the record carrying it left it.
///
/// After the source model refuses, the next `remaining` responses come from `fallback`; then
/// the session probes the source again, counting clean source responses in `clean`. A refusal
/// during the probe doubles the next round (10, 20, then 40), and ten clean source responses
/// make the next refusal start again at 10.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusalFallbackSchedule {
    /// The model whose refusal armed the schedule.
    pub source: ModelHandle,
    /// The model refused steps and scheduled steps are sent to.
    pub fallback: ModelHandle,
    pub phase: RefusalFallbackPhase,
    /// How many refusals came during a probe that had not yet seen ten clean source responses;
    /// saturates at 2.
    pub re_refusals: u8,
    /// Wall-clock time of the record that last changed the schedule, Unix milliseconds. Used
    /// to expire a schedule left idle for a day.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_ms: Option<u64>,
}

/// Where a refusal-fallback schedule stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RefusalFallbackPhase {
    /// Steps go to the fallback model; `remaining` more committed fallback steps end the round.
    Fallback { remaining: u32 },
    /// Steps go to the source model again; `clean` counts its responses since the round ended,
    /// capped at 10.
    Probe { clean: u32 },
}
