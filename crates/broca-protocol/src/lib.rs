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

pub mod approval;
pub mod caller;
pub mod canonical;
pub mod content;
mod diagnostic;
pub mod display;
pub mod error;
pub mod ids;
pub mod provider;
pub mod request;
pub mod session;
pub mod usage;

// Convenience re-exports for the common wire surface.
pub use approval::{ApprovalRequest, ApprovalResponse};
pub use caller::{ControlUnit, PauseReason, RunFinishReason};
pub use canonical::canonical_json_bytes;
pub use content::{
    AssistantMessage, ContentBlock, EncryptedReasoningFormat, FinishReason, Message, Role,
    ToolCall, ToolOutput, ToolResult,
};
pub use diagnostic::diag;
pub use display::DisplayEvent;
pub use error::{ErrorClass, ProviderError, ACCOUNT_LIMIT_EXHAUSTED, BILLING_EXHAUSTED};
pub use ids::{BatchId, MessageId, RunId, StepId, StreamGen, SubmissionId, ToolCallId};
pub use provider::{
    AttemptAccountLimit, AttemptAccountObservation, AttemptOutcome, CredentialRefresh,
    InputTransformation, ModelAttempt, NormalizedEvent, ProviderControl,
    RATELIMIT_UNIFIED_HEADER_PREFIX,
};
pub use request::{
    AuthRequest, BudgetConfig, BudgetKind, BudgetNudge, CacheMode, CacheRequest, CacheRole,
    CacheTail, CallOptions, Delivery, ExecutionMode, GenerationConfig, ModelHandle, ResponseFormat,
    StepContext, StopCondition, SubmissionRequest, Tool, ToolChoice,
};
pub use session::{BindIdentity, BindPrincipal, BindScope, SendScope, SessionScope};
pub use usage::Usage;
