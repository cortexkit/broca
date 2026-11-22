//! What live code still uses from the old log format, whose file store is gone.
//!
//! The record vocabulary (re-exported from `broca-records`; the session store's own record
//! kinds are in `broca-session-store`), the frame codec and its must-understand feature names,
//! the `state-format` marker check at startup, and size limits.
#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::disallowed_methods))]
// Test fixtures intentionally use direct I/O to prepare and inspect crash images.
#![cfg_attr(test, allow(clippy::disallowed_methods))]

pub mod framing;
pub use broca_records::{floor, fs_ops, record, session_storage};
pub mod state_format;

pub use floor::{raise_floor, read_floor, FEATURE_FLOOR_FILENAME};
#[cfg(any(test, feature = "test-known-feature"))]
pub use framing::{forget_feature_for_test, ForgottenFeature};
pub use framing::{
    FrameError, FramedRecord, WalLineageId, ACCOUNT_LIMIT_PAUSE_FEATURE, ARCHIVE_INDEX_V2_FEATURE,
    COMPACTION_FEATURE, COMPACTION_V2_FEATURE, COMPACTION_VIEW_FEATURE, COMPACTION_WAIT_FEATURE,
    DEFERRED_CALL_FEATURE, DISPATCH_MODULE_FEATURE, FRAME_STAMPS_FEATURE, FRAMING_VERSION,
    GATED_FRAMING_VERSION, HEADER_LEN, KNOWN_FEATURES, MAX_PAYLOAD_LEN, MESSAGE_TIMES_FEATURE,
    PLAN_MANIFEST_FEATURE, PLAN_REFERENCE_SLOTS_FEATURE, PROVIDER_DECLARATIONS_FEATURE,
    PROVIDER_FALLBACK_BLOCK_FEATURE, PROVIDER_HELD_FEATURE, REFUSAL_FALLBACK_FEATURE,
    RUNNER_TAGS_FEATURE, RUNNER_TAGS_V2_FEATURE, SCOPE_FEATURE, SERVICE_TIER_QUEUE_FEATURE,
    SESSION_CHANGE_FEATURE, SESSION_STORAGE_DIR, SESSION_STORAGE_FEATURE, STEER_QUEUE_FEATURE,
    STEP_TRANSFORMS_FEATURE, TOOL_NAME_ALIASES_FEATURE, TOOL_REPLY_FEATURE, TOOL_TIMES_FEATURE,
};
pub use record::{
    compaction_replacement_sha256, AccountDecision, AccountLimitPause, ApprovalDecision,
    AuthSelection, AwaitingApprovalEntry, CompactionCursor, CompactionDisposition,
    CompactionMessageKind, CompactionRange, CompactionStabilityRank, CompactionStrip,
    CompactionViewChoice, CompactionViewV2, CompactionWait, CompactionWaitExit, ComputedTagRules,
    ConversationKeySource, DeferredAsk, DeferredDecision, DeferredExecutor, DispatchOutcome,
    DispatchSchemaPin, FetchOutcome, FrozenDeclaration, FrozenHoldArgument,
    FrozenProviderDeclarations, FrozenReferenceSlot, FrozenReply, FrozenResultFormat, HookOp,
    Hooks, LateDecision, LateExecution, LateOutcome, NoDispatchReason, OnExpiry, OpaqueJson,
    PlanManifest, ProviderFetch, ProviderHeld, QueuedAdmissionFailure, RefusalFallbackPhase,
    RefusalFallbackSchedule, RejectedCompaction, ResumeRedo, ResumeReplayed, RunConfig, RunOrigin,
    SeedOriginKind, SetupTagCheckpoint, SetupViewDue, StepFallback, StepRoute, StepRouteReason,
    StoredViewEdit, UsageIteration, VaultReadOp, VaultRequest, WalRecord, WithdrawElicitation,
    WithdrawalAnswer, ACCOUNT_DECISION_AUTH_REQUIRED, ACCOUNT_DECISION_CANDIDATES_UNKNOWN,
    ACCOUNT_DECISION_DISABLED, ACCOUNT_DECISION_LEGACY_STATIC, ACCOUNT_DECISION_PINNED,
    ACCOUNT_DECISION_ROUTED, ACCOUNT_DECISION_SINGLE_CANDIDATE, ACCOUNT_DECISION_UNREACHABLE,
    ACCOUNT_DECISION_UNSUPPORTED, MAX_REPLACEMENT_BYTES,
};
pub use session_storage::{
    placement, selected_for_backup, session_storage_class, session_storage_class_by,
    session_storage_head, session_storage_moves, SessionInTwoClasses, StorageClass,
    MOVE_STAGING_DIR, SESSION_IN_TWO_CLASSES, SESSION_STORAGE_DURABLE_DIR,
    SESSION_STORAGE_EPHEMERAL_DIR, SESSION_STORAGE_MOVES_DIR,
};
pub use state_format::{classify_state_root, StateRootError, WalConfig, WalFormat};
// `PauseReason` is shared caller/WAL vocabulary; it lives in `broca-protocol` (like
// `RunFinishReason`) and is re-exported here so existing `broca_wal::PauseReason` paths
// keep resolving.
pub use broca_protocol::PauseReason;

pub mod size_limit;
