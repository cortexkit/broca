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
pub mod caller;
pub mod canonical;
pub mod content;
mod diagnostic;
pub mod display;
pub mod error;
pub mod ids;
pub mod provider;
pub mod render_provenance;
pub mod request;
pub mod session;
pub mod structured_output;
pub mod tags;
pub mod usage;

#[cfg(test)]
mod media_tests;

// Convenience re-exports for the common wire surface.
pub use approval::{ApprovalRequest, ApprovalResponse};
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
    AuthRequest, BudgetConfig, BudgetKind, BudgetNudge, CacheMode, CacheRequest, CacheRole,
    CacheTail, CallOptions, Delivery, DeliveryMode, DeliveryOrder, ExecutionMode, GenerationConfig,
    ModelHandle, ResponseFormat, StepContext, StopCondition, SubmissionRequest, Tool, ToolChoice,
    ToolReply,
};
pub use session::{
    BindIdentity, BindPrincipal, BindScope, ParentState, ScopeKind, ScopeParent, SendScope,
    SessionScope,
};
pub use usage::Usage;
pub mod run_events;

#[cfg(all(feature = "test-attachments", not(debug_assertions)))]
compile_error!("test-attachments is a non-shipping feature");

/// Attachment admission is available only in explicitly opted-in debug test builds.
pub fn attachments_enabled() -> bool {
    #[cfg(all(feature = "test-attachments", debug_assertions))]
    {
        std::hint::black_box("broca-nonshipping-attachments-v1");
        true
    }
    #[cfg(not(all(feature = "test-attachments", debug_assertions)))]
    {
        false
    }
}
