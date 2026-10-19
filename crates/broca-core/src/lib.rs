//! broca core — the agentic loop and its seams.
//!
//! The [`run::run_loop`] (stop_when after every step), tool dispatch through the
//! [`tool_plane::ToolPlane`] seam, the model-step [`provider::Provider`] seam, the
//! capability manifest + approval gate, and the durable single-writer [`lease`].
//! Provider display deltas are passed through; a changed post-assistant message is
//! re-streamed after a display reset. Every seam is a trait so a deterministic stub
//! can swap for the real implementation with no loop changes.
#![forbid(unsafe_code)]

/// The pinned role types and rules shared by compaction consumers without extra dependencies.
pub mod compaction_provider;

pub mod account_report;
pub mod admission;
pub mod billing;
mod budget_kernel;
pub mod cache_warm;
pub mod capability;
pub mod caps;
pub mod clock;
pub mod compaction;
pub mod consent;
pub mod context;
pub mod delivery_order;
pub mod hooks;
pub mod import;
pub mod input_budget;
pub mod lease;
pub mod projection;
mod projection_health;
pub mod projection_plan;
#[cfg(test)]
mod projection_plan_tests;
pub mod provider;
pub mod queue;
pub mod render;
pub mod replay;
pub use context::StorePageReadError;
pub use replay::RunAttribution;
pub mod resume;
pub mod retention;
pub mod run;
pub mod scope_admission;
pub mod shutdown_seal;
pub mod steer;
pub mod stub_plane;
pub mod tags;
pub mod tool_plane;
pub mod tool_repetition;
pub mod wal_fold;
pub mod wal_writer;

pub use admission::{
    decode_admission_checkpoint, encode_admission_checkpoint, send_identity_difference,
    send_identity_difference_with_conversation_key, send_identity_digest,
    send_identity_digest_with_conversation_key, submission_difference,
    submission_identity_difference, submission_identity_digest, AdmissionCheckpointV1,
    BudgetReplayStateV1, EpisodeDisposition, FirstEpisodeV1, LastEpisodeV1, SessionOwnershipV1,
    BUDGET_REPLAY_STATE_VERSION,
};
pub use billing::{
    charge_basis_overrides_digest, compute_segments, compute_segments_for_run, imported_run_ids,
    ChargeBasis, ChargeBasisResolver, SpendSegment, TimestampPrecision, NO_CHARGE_BASIS_OVERRIDES,
    USAGE_POLICY_VERSION,
};
pub use capability::{
    AllowAll, ApprovalPolicy, CapabilityViolation, DenyAll, ScriptedApproval, ToolManifest,
};
pub use caps::{
    AppliedCapWatermark, CapBaseline, CapInstallRequest, CapMutation, CapOperation, CapRow,
    CapStore, CapStoreError, CapWatermark, SpendDeltaRequest,
};
pub use clock::Clock;
pub use compaction::{
    CompactionCheckpoint, CompactionInbox, CompactionPlane, CompactionState, CompactionUnavailable,
    MAX_REPLACEMENT_BYTES,
};
pub use context::{
    call_key, catch_up_usage_exports, dispatches_of_turns, effective_context,
    emit_lineage_usage_exports, read_session_page, read_session_page_owned,
    read_session_page_owned_with_dispatches, record_time_ms, replay_transcript,
    resume_state_from_store, tool_call_dispatches, AdmissionStoreSnapshot, ExportCursor,
    ExportError, ExportFact, ExportIndex, ExportPage, MaterializedStore, MissingExportRun,
    Persister, RunIndex, RunIndexRow, RunIndexState, SessionReadLineageState, SessionReadMessage,
    SessionReadPage, SessionReadReason, SessionReadStateKind, SessionStore, StorePageRow,
    StorePageSnapshot, StorePrefix, TagLocation, ToolCallDispatch, WithdrawalRun,
    SESSION_READ_DEFAULT_LIMIT, SESSION_READ_MAX_LIMIT,
};
pub use context::{
    read_session_model_page, ModelEntry, ModelPage, ModelPageError, ModelPageRequest, ReadView,
};
pub use hooks::{HookPlane, HookUnavailable, STEP_TRANSFORM_HOOK_CAP};
pub use import::{
    check_import_retry, run_import, run_import_with_store, ImportError, ImportOrigin,
    ImportOutcome, ImportRequest, IMPORT_MAX_MESSAGES, IMPORT_MAX_SERIALIZED_BYTES,
};
pub use lease::{lease_key_for, FileLeaseStore, LeaseError, LeaseHandle, LeaseStore};
pub use projection::{
    advance_control_projection_state, classify_control_unit, last_control_cursor_through,
    project_control, project_control_with_state, ControlClass, ControlProjectionState, Cursor,
};
pub use provider::{Provider, StubProvider};
pub use queue::{
    insert_in_drain_order, pending_submissions, retract_decision, PendingSubmission,
    RetractDecision,
};
pub use render::{
    canonical_request_bytes, request_digest, request_digest_hex, CANONICAL_RENDERER_VERSION,
};
pub use replay::{
    budget_replay_seed_is_valid, deferred_call_obligations, latest_projection_boundary,
    no_dispatch_result, reconcile_run_state, replay, replay_projection_tail,
    DeferredCallObligations, DeferredCallProjection, LateApprovalView, OpenCallState,
    OpenToolBatch, OwedWithdrawal, PendingTurn, PreparedCall, ProjectedDeferredCall,
    ProjectionDelta, ProjectionSeed, ReconciledRunState, ReplayError, ReplayResolution,
    ReplayState, RunUsageProjection,
};
pub use resume::{
    close_out_open_batch, not_dispatched_result, outcome_unknown_result, resume_started_record,
    DeferredCloseKind,
};
pub use retention::{is_retention_lineage, LineageRetention};
pub use scope_admission::{
    decide_scope, AdoptionRefusal, OwnerMismatch, ScopeDecision, ScopeRefusal,
};
// Re-exported so resume/context APIs can name the authoritative request config + the
// frozen auth-selection a caller threads into a fresh run's RunControl.
pub use account_report::AccountReporter;
pub use broca_wal::{AccountDecision, AuthSelection, RunConfig, VaultReadOp, VaultRequest};
// Re-exported so resume APIs can name the run id recovered for the last episode.
pub use broca_protocol::RunId;
pub use cache_warm::{
    CacheClock, CacheClockReading, RecordedRequest, RecordedRequestError, SentRequest, WarmFailure,
    WarmSent,
};
pub use run::{
    input_transformations_reported, input_transformations_unmeasured_attempts,
    load_prior_wal_snapshot, read_prior_run_config, recover_prior_run_config, run_loop,
    run_loop_append_episode, run_loop_on_writer, run_loop_on_writer_admitted,
    run_loop_on_writer_admitted_with_prior_snapshot, run_loop_resume_after_auth,
    run_loop_resume_after_auth_with_control, run_loop_under_lease, run_loop_with_control,
    run_loop_with_store, AccountSelectionRefusal, OutputEvent, PriorWalSnapshot, RunControl,
    RunError, RunRequest, RunSummary, StartMode,
};
pub use shutdown_seal::{SealedAttempt, ShutdownSeal};
pub use steer::{OrderedAcceptance, SteerInbox};
pub use stub_plane::{StubEvent, StubReply, StubToolPlane};
pub use tags::LineageTags;
pub use tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError};
pub use wal_writer::{
    agent_session_ref, session_addr, FileWalStore, ObservedRecord, ReadOnlyWalReader,
    RecordObserver, RecordOutcome, SessionRefIndex, WalHandle, WalOpenError, WalStore,
    WalWriteError, WAL_MANIFEST_FILENAME,
};
pub use wal_writer::{
    session_projection_incomplete, ProjectionAdmission, ProjectionRepair,
    SESSION_PROJECTION_INCOMPLETE,
};

pub mod keep_warm;
pub mod keep_warm_wait;
