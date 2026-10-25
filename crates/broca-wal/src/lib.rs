//! broca WAL — the intent-log source of truth.
//!
//! Framed records (len/ver/seq/fence/SHA-256 digest), the durable record set
//! (`record::WalRecord` — semantics authoritative in
//! `docs/durability-contract.md`), and [`FileWal`] behind the [`Wal`] trait. The
//! writer task, replay state machine, and materialized store build on top of this
//! foundation.
#![forbid(unsafe_code)]

pub mod archive;
mod directory_sync;
pub mod file_wal;
pub mod floor;
pub mod framing;
pub mod record;

pub use file_wal::{
    file_identity, format_manifest_line, read_records_range, read_records_range_from_valid_anchor,
    read_records_tail, replay_file, wal_bytes_read, wal_file_exists, FileWal, Wal, WalByteAnchor,
    WalError, WalRangeRead, WalRangeRecords, WalTailRead, DEFAULT_MAX_READ_BYTES,
};
pub use floor::{raise_floor, read_floor, FEATURE_FLOOR_FILENAME};
#[cfg(any(test, feature = "test-known-feature"))]
pub use framing::{forget_feature_for_test, ForgottenFeature};
pub use framing::{
    FrameError, FramedRecord, WalLineageId, ARCHIVE_INDEX_V2_FEATURE, COMPACTION_FEATURE,
    COMPACTION_VIEW_FEATURE, COMPACTION_WAIT_FEATURE, DEFERRED_CALL_FEATURE,
    DISPATCH_MODULE_FEATURE, FRAMING_VERSION, GATED_FRAMING_VERSION, HEADER_LEN, KNOWN_FEATURES,
    MAX_PAYLOAD_LEN, MESSAGE_TIMES_FEATURE, PLAN_MANIFEST_FEATURE, PROVIDER_DECLARATIONS_FEATURE,
    PROVIDER_HELD_FEATURE, RUNNER_TAGS_FEATURE, RUNNER_TAGS_V2_FEATURE, SCOPE_FEATURE,
    STEER_QUEUE_FEATURE, STEP_TRANSFORMS_FEATURE, TOOL_REPLY_FEATURE, TOOL_TIMES_FEATURE,
};
pub use record::{
    compaction_replacement_sha256, AccountDecision, ApprovalDecision, AuthSelection,
    AwaitingApprovalEntry, CompactionCursor, CompactionDisposition, CompactionMessageKind,
    CompactionRange, CompactionStabilityRank, CompactionStrip, CompactionViewChoice,
    CompactionWait, CompactionWaitExit, ComputedTagRules, ConversationKeySource, DeferredAsk,
    DeferredDecision, DeferredExecutor, DispatchOutcome, DispatchSchemaPin, FetchOutcome,
    FrozenDeclaration, FrozenHoldArgument, FrozenProviderDeclarations, FrozenReply,
    FrozenResultFormat, HookOp, Hooks, LateDecision, LateExecution, LateOutcome, NoDispatchReason,
    OnExpiry, OpaqueJson, PlanManifest, ProviderFetch, ProviderHeld, QueuedAdmissionFailure,
    RejectedCompaction, ResumeRedo, ResumeReplayed, RunConfig, RunOrigin, SeedOriginKind,
    SetupTagCheckpoint, VaultReadOp, VaultRequest, WalRecord, WithdrawElicitation,
    WithdrawalAnswer, ACCOUNT_DECISION_AUTH_REQUIRED, ACCOUNT_DECISION_CANDIDATES_UNKNOWN,
    ACCOUNT_DECISION_DISABLED, ACCOUNT_DECISION_LEGACY_STATIC, ACCOUNT_DECISION_PINNED,
    ACCOUNT_DECISION_ROUTED, ACCOUNT_DECISION_SINGLE_CANDIDATE, ACCOUNT_DECISION_UNREACHABLE,
    ACCOUNT_DECISION_UNSUPPORTED, MAX_REPLACEMENT_BYTES,
};
// `PauseReason` is shared caller/WAL vocabulary; it lives in `broca-protocol` (like
// `RunFinishReason`) and is re-exported here so existing `broca_wal::PauseReason` paths
// keep resolving.
pub use broca_protocol::PauseReason;
pub mod migration;

pub mod size_limit;
