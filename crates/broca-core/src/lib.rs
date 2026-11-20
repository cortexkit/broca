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
pub mod attempt_id;
pub mod billing;
pub mod budget_kernel;
pub use budget_kernel::{next_due, DueFiring};
pub mod cache_warm;
/// Which copy of a tool call each durable record answers, and the key today's readers give it.
pub mod call_identity;
pub mod capability;
pub mod clock;
pub mod compaction;
pub mod compaction_v2;
pub mod consent;
pub mod context;
pub mod creation_times;
pub mod delivery_order;
pub mod display_facts;
pub mod durability_counters;
pub mod fresh_start;
pub mod head;
pub use head::is_mode3_log;
pub mod held_calls;
pub mod hooks;
pub mod import;
pub mod input_budget;
pub mod input_transformations;
pub mod lease;
pub mod media_provenance;
pub mod media_request;
pub mod media_store;
pub mod media_uploads;
pub mod media_validation;
pub mod message_id;
pub mod message_times;
pub use message_id::{message_mid, position_of_mid};
pub mod output_events;
pub mod output_reservation;
pub mod placement;
pub mod projection;
pub mod projection_plan;
pub mod provider;
pub mod provider_errors;
pub mod queue;
pub mod refusal_fallback;
pub mod render;
pub mod repetition;
pub mod replay;
pub mod seal;
pub mod served_history;
pub use served_history::ServedHistory;

pub use context::state_for_terminal;
pub use context::StorePageReadError;
pub use replay::RunAttribution;
pub mod resume;
pub mod retention;
pub mod run;
pub mod scope_admission;
pub mod shutdown_seal;
pub mod signed_floor;
pub mod step_usage;
pub mod stop_conditions;
pub mod structured_output;
pub mod stub_plane;
pub mod system_one_fact;
pub mod tags;
pub mod tool_media;
pub mod tool_plane;
pub mod tool_repetition;
pub mod tool_results;
pub mod wal_writer;

pub use admission::{
    digest_client_message_id, send_identity_difference_with_conversation_key,
    send_identity_digest_with_conversation_key, submission_difference, submission_identity_digest,
    BudgetReplayStateV1, FirstEpisodeV1, SessionOwnershipV1, BUDGET_REPLAY_STATE_VERSION,
};

pub use admission::{
    send_identity_digest, AdmissionCheckpointV1, EpisodeDisposition, LastEpisodeV1,
};
pub use billing::{
    charge_basis_overrides_digest, compute_segments_for_run, imported_run_ids, ChargeBasis,
    ChargeBasisResolver, ModelShare, SpendSegment, NO_CHARGE_BASIS_OVERRIDES,
};
pub use billing::{compute_run_billing, RunBilling, TimestampPrecision, USAGE_POLICY_VERSION};
pub use capability::CapabilityViolation;
pub use capability::{AllowAll, ApprovalPolicy, ToolManifest};
pub use clock::Clock;
pub use compaction::CompactionState;
pub use compaction::{
    CompactionCheckpoint, CompactionPlane, CompactionUnavailable, CompactionViews,
    ProviderCapabilities, ViewReadyInbox, MAX_REPLACEMENT_BYTES,
};
pub use context::{
    emit_lineage_usage_exports, replay_transcript, ExportCursor, ExportError, ExportFact,
    ExportIndex, ExportPage, ImportedRunExclusions, MissingExportRun, RecoveryDiscovery,
    RunEventBodies, RunIndex, RunIndexAttribution, RunIndexRow, RunIndexState, SessionInventory,
    SessionReadLineageState, SessionReadMessage, SessionReadPage, SessionReadReason,
    SessionReadStateKind, SessionRefResolver, SessionRefs, TagLocation, ToolCallDispatch,
    WithdrawalRun, SESSION_READ_DEFAULT_LIMIT, SESSION_READ_MAX_LIMIT,
};
pub use context::{
    AdmissionStoreSnapshot, HeldEpisode, HeldPrefix, HeldRowsRequest, MaterializedStore, Persister,
    SessionStore, StorePageRow, StorePageSnapshot, StorePrefix,
};
pub use context::{ModelEntry, ReadView};
pub use hooks::{HookPlane, HookUnavailable, STEP_TRANSFORM_HOOK_CAP};
pub use import::{
    ImportError, ImportOrigin, ImportOutcome, ImportRequest, IMPORT_MAX_MESSAGES,
    IMPORT_MAX_SERIALIZED_BYTES,
};
pub use lease::{lease_key_for, FileLeaseStore, LeaseError, LeaseHandle, LeaseStore};
pub use projection::ControlProjectionState;
pub use projection::Cursor;
pub use provider::{Provider, StubProvider};
pub use queue::pending_submissions;
pub use queue::{insert_in_drain_order, PendingSubmission};
pub use render::CANONICAL_RENDERER_VERSION;
pub use render::{canonical_request_bytes, request_digest, request_digest_hex};
pub use replay::{
    deferred_call_obligations, reconcile_run_state, replay, LateApprovalView, OpenCallState,
    ReconciledRunState, ReplayError,
};
pub use replay::{
    DeferredCallObligations, DeferredCallProjection, OpenToolBatch, OwedWithdrawal, PendingTurn,
    PreparedCall, ProjectedDeferredCall, ProjectionDelta, ProjectionSeed, ReplayResolution,
    ReplayState,
};
pub use resume::DeferredCloseKind;
pub use retention::is_retention_lineage;
pub use retention::LineageRetention;
pub use scope_admission::{
    decide_scope, AdoptionRefusal, OwnerMismatch, ScopeDecision, ScopeRefusal,
};
// Re-exported so resume/context APIs can name the authoritative request config + the
// frozen auth-selection a caller threads into a fresh run's RunControl.
pub use account_report::AccountReporter;
pub use broca_wal::{AccountDecision, AuthSelection, RunConfig, VaultReadOp, VaultRequest};
// Re-exported so resume APIs can name the run id recovered for the last episode.
pub use broca_protocol::RunId;
pub use cache_warm::RecordedRequestError;
pub use cache_warm::{
    CacheClock, CacheClockReading, RecordedRequest, SentRequest, WarmFailure, WarmSent,
};
pub use input_transformations::{
    input_transformations_reported, input_transformations_unmeasured_attempts,
};
pub use output_events::OutputEvent;
pub use run::{
    AccountSelectionRefusal, RoutingUnavailable, RunControl, RunError, RunRequest, RunSummary,
    StartMode,
};
pub use run::{PriorWalSnapshot, RoutingUnavailableReason};
pub use shutdown_seal::SealedAttempt;
pub use shutdown_seal::ShutdownSeal;
pub use step_usage::RunUsageProjection;
pub use stub_plane::StubEvent;
pub use stub_plane::{StubReply, StubToolPlane};
pub use tags::LineageTags;
pub use tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError};
pub use tool_results::outcome_unknown_result;
pub use tool_results::{no_dispatch_result, not_dispatched_result};
pub use wal_writer::ProjectionAdmission;
pub use wal_writer::{
    agent_session_ref, session_addr, FileWalStore, ReadOnlyWalReader, RecordObserver,
    SessionRefIndex, WalHandle, WalStore,
};
pub use wal_writer::{
    session_projection_incomplete, ProjectionRepair, SESSION_PROJECTION_INCOMPLETE,
};
pub use wal_writer::{
    session_storage_refusal, ObservedRecord, RecordOutcome, WalWriteError, WAL_MANIFEST_FILENAME,
};

pub mod account_limit_pause;
pub use account_limit_pause::{
    account_limit_pause_declined, account_limit_stale_resets, session_account_cache_expiry_ms,
    SameAccountWait,
};
pub mod keep_warm;
pub mod keep_warm_wait;
pub mod plan_system;
