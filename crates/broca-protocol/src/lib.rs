//! broca canonical wire contract.
//!
//! Two distinct schemas:
//!  - provider module → runner [`provider::ProviderControl`] (assembled control
//!    units) + [`display::DisplayEvent`] (lossy deltas).
//!  - runner → caller [`caller::ControlUnit`] (the OutputStream control units) +
//!    the same [`display::DisplayEvent`] passthrough.
//!
//! Canonical wire types in this crate contain only serde-serializable data, and the crate also
//! provides a standard-library-only diagnostic helper. The direct benchmark socket uses a
//! two-lane framing protocol inside a subc envelope; its frame codec is implemented in
//! `broca-replay`, while production code transports these payloads through the subc management
//! routes in `broca-module-serve`.
#![forbid(unsafe_code)]

/// Admission and execution share a 30 s ceiling so frozen hook budgets are executable.
pub const STEP_TRANSFORM_HOOK_CAP: u64 = 30_000;

/// Shared `session.read` reply-body budget: 3 MiB, before transport framing.
pub const READ_REPLY_MAX_BYTES: u64 = 3 * 1024 * 1024;

/// Count the exact bytes a value occupies as compact JSON without building a second body buffer.
/// Serialization failures return `u64::MAX`, which callers treat as over budget.
pub fn serialized_json_bytes(value: &impl serde::Serialize) -> u64 {
    struct Count(u64);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len() as u64);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_or(u64::MAX, |()| count.0)
}

pub mod approval;
pub mod auth_method;
pub mod caller;
pub mod caller_message;
pub mod canonical;
pub mod content;
mod diagnostic;
pub mod display;
pub mod error;
pub mod ids;
pub mod json;
pub mod media_event;
pub mod provider;
pub mod render_hash;
pub mod render_provenance;
pub mod request;
pub mod session;
pub mod session_change;
pub mod structured_output;
pub mod tags;
pub mod text;
pub mod tool_name_alias;
pub mod usage;
pub mod wire_node;

#[cfg(test)]
mod media_tests;

// Convenience re-exports for the common wire surface.
pub use approval::{ApprovalRequest, ApprovalResponse};
pub use auth_method::AuthMethod;
pub use caller::{
    ControlUnit, OpenPauseReason, OpenRunFinishReason, OpenState, PauseReason, RunFinishReason,
};
pub use canonical::canonical_json_bytes;
pub use content::{
    bound_module_unavailable_detail, AssistantMessage, ContentBlock, EncryptedReasoningFormat,
    FinishReason, MediaKind, MediaMarkerReason, MediaRef, Message, Role, TextProviderFormat,
    TextProviderState, ToolCall, ToolOutput, ToolOutputPart, ToolResult,
    MODULE_UNAVAILABLE_DETAIL_MAX_BYTES, MODULE_UNAVAILABLE_RETRY_MAX_BYTES,
    TOOL_PLANE_UNAVAILABLE_MESSAGE, TOOL_PLANE_UNAVAILABLE_RETRY,
};
pub use cortexkit_role_llm_runner::errors::ContextCeilingDetail;
pub use cortexkit_role_llm_runner::message_marks::{MessageMarks, SpeaksFor};
pub use cortexkit_role_llm_runner::rich_ui::RichUiStamp;
pub use cortexkit_role_llm_runner::run::BudgetExhaustedDetail;
pub use diagnostic::diag;
pub use display::DisplayEvent;
pub use error::{
    ErrorClass, OpenErrorClass, ProviderError, ACCOUNT_LIMIT_EXHAUSTED, BILLING_EXHAUSTED,
};
pub use ids::{BatchId, MessageId, RunId, StepId, StreamGen, SubmissionId, ToolCallId};
pub use provider::{
    AttemptAccountLimit, AttemptAccountObservation, AttemptOutcome, CredentialRefresh,
    InputTransformation, ModelAttempt, NormalizedEvent, ProviderControl,
    RATELIMIT_UNIFIED_HEADER_PREFIX,
};
pub use request::{
    AuthRequest, BudgetConfig, BudgetKind, BudgetNudge, CacheTail, CallOptions, Delivery,
    DeliveryMode, DeliveryOrder, ExecutionMode, GenerationConfig, ModelHandle, ResponseFormat,
    StepContext, StopCondition, SubmissionRequest, Tool, ToolChoice, ToolReply,
};
pub use session::{
    canonical_module, canonical_owner, other_module_name, same_module, same_owner_string,
    BindIdentity, BindPrincipal, BindScope, ParentState, ScopeKind, ScopeParent, SendScope,
    SessionScope, RENAMED_MODULES,
};
pub use usage::Usage;
pub mod run_events;
pub mod send_states;

#[cfg(all(feature = "test-attachments", not(debug_assertions)))]
compile_error!("test-attachments is a non-shipping feature");

/// Whether this build serves llm-runner `fork_answer` (`session.fork_prepare`, `fork_ask`,
/// `fork_end`). `role.describe` declares the group, and admission checks a plan's
/// `fork_callers`, exactly when it is set; without it the member stays uninterpreted.
pub const FORK_ANSWER_SERVED: bool = true;

/// `fork_idle_s`: how long a fork lives after its prepare or its last answered ask.
pub const FORK_IDLE_S: u64 = 600;

static SERVICE_TIER_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Fix service-tier admission and discovery before serving requests. Restart to change it.
pub fn configure_service_tier(enabled: bool) -> Result<(), &'static str> {
    SERVICE_TIER_ENABLED
        .set(enabled)
        .map_err(|_| "service_tier.enabled was already configured; restart broca to change it")
}

pub fn service_tier_enabled() -> bool {
    SERVICE_TIER_ENABLED.get().copied().unwrap_or(false)
}

/// Recover the abstract applied level from the recorded request bytes, never today's catalog.
pub fn frozen_service_tier(render: &serde_json::Value) -> &'static str {
    match render
        .get("service_tier")
        .and_then(serde_json::Value::as_str)
    {
        Some("ultrafast") => "ultrafast",
        Some("priority" | "fast") => "fast",
        _ if render.get("speed").and_then(serde_json::Value::as_str) == Some("fast") => "fast",
        _ => "standard",
    }
}
