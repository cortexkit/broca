//! broca as a subc module: the session-lineage coordinator.
//!
//! One in-process ACTOR per open session owns that session's durable substrate (lease +
//! WAL + store partition) for its lifetime and runs the agentic loop on behalf of
//! consumers. A process-global registry admits exactly one actor per session and
//! mediates open/close. Fan-out streams a run's control/display events to subscribers
//! with per-subscriber backpressure. The `session.*`/`run.*` serve types live here; subc
//! owns the wire mechanics (see `docs/lineage-actor-coordinator.md`).
#![forbid(unsafe_code)]

pub mod actor;
pub mod cache;
pub mod claude_code_identity;
mod compaction_watch;
pub mod diagnostics;
pub mod executor;
pub mod fanout;
pub mod head_time;
pub mod held_slots;
pub mod held_withdrawals;
pub mod history;
pub mod late_results_puller;
pub mod late_results_store;
pub mod opener;
pub mod output_refs;
pub mod person_media;
#[cfg(test)]
mod phase_metrics_tests;
mod plan_change;
mod plan_episode;
pub mod registry;
pub mod retention;
pub mod retention_withdrawals;
mod run_admission;
mod send_fate;
mod sender_admission;
pub mod serve;
pub mod session_class;
pub mod session_cmd;
pub mod step_events;
pub mod store_actor;
pub mod store_billing;
pub mod store_calls;
pub mod store_collect;
pub mod store_control;
pub mod store_entries;
pub mod store_fanout;
pub mod store_head;
pub mod store_import;
pub mod store_media;
pub mod store_open;
pub mod store_output_refs;
mod store_plan_change;
pub mod store_read;
pub mod store_reads;
pub mod store_status;
pub mod store_tool_calls;
mod uploads;
pub mod wal_render;

pub use cache::{CacheDeclaration, CacheDeclarer};
pub use executor::{RunExecutor, RunJob, RunKind, SealedRun, WarmBase, WarmError, WarmJob};
pub use fanout::{Delivery, FromCursor};
#[cfg(feature = "test-clock")]
pub use keep_warm::{automatic_warmer_for_test, warm_setup_for_allocation_test};
pub use opener::DefaultOpener;
pub use registry::{LineageKey, LineageRegistry};
pub use serve::{
    CancelResponse, RetractResponse, SendRequest, SendResponse, ServeError, ServeHandler,
    SessionImportRequest, SessionImportResponse, SessionReadLineageState, SessionReadMessage,
    SessionReadRequest, SessionReadResponse, StatusResponse, SubscribeFrom,
};

mod approval;
mod approval_state;
pub mod cut_state;
mod durable_read;
mod keep_warm;
pub mod move_files;
pub mod move_install;
mod source_warm;

#[cfg(test)]
mod history_version_tests;
