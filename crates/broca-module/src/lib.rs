//! broca as a subc module: the session-lineage coordinator.
//!
//! One in-process ACTOR per open session owns that session's durable substrate (lease +
//! WAL + store partition) for its lifetime and runs the agentic loop on behalf of
//! consumers. A process-global registry admits exactly one actor per session and
//! mediates open/close. Fan-out streams a run's control/display events to subscribers
//! with per-subscriber backpressure. The `session.*`/`run.*` serve types live here; subc
//! owns the wire mechanics (see `docs/lineage-actor-coordinator.md`).
#![forbid(unsafe_code)]

// The memory budget tests count what their thread allocates (see `broca_test_support::alloc`).
// Only the test binary installs the counting allocator.
#[cfg(test)]
#[global_allocator]
static ALLOCATOR: broca_test_support::alloc::CountingAlloc =
    broca_test_support::alloc::CountingAlloc;

pub mod actor;
pub mod cache;
pub mod claude_code_identity;
pub mod diagnostics;
pub mod executor;
pub mod fanout;
pub mod fork_answer;
pub mod fork_spend;
pub mod fork_state;
pub mod head_time;
pub mod held_slots;
pub mod held_withdrawals;
pub mod history;
pub mod late_results_puller;
pub mod late_results_store;
pub mod memory_report;
pub mod opener;
pub mod output_refs;
pub mod person_media;
#[cfg(test)]
mod phase_metrics_tests;
mod plan_change;
mod plan_episode;
pub mod reader_admission;
pub mod registry;
pub mod retention;
mod run_admission;
mod send_fate;
mod sender_admission;
pub mod serve;
pub mod session_class;
pub mod session_cmd;
pub mod step_events;
pub mod store_actor;
pub mod store_awaits;
pub mod store_billing;
pub mod store_calls;
pub mod store_collect;
pub mod store_control;
pub mod store_entries;
pub mod store_exposure;
pub mod store_fanout;
pub mod store_head;
pub mod store_import;
pub mod store_import_stage;
pub mod store_media;
pub mod store_open;
pub mod store_output_refs;
mod store_plan_change;
pub mod store_plan_view;
pub mod store_publish;
pub mod store_read;
pub mod store_reads;
pub mod store_request_dump;
pub mod store_run_events;
pub mod store_send_states;
pub mod store_status;
pub mod store_tag_map;
pub mod store_tag_read;
pub mod store_tool_calls;
mod uploads;
pub mod wal_render;

pub use cache::{CacheDeclaration, CacheDeclarer};
pub use executor::{RunExecutor, RunKind, SealedRun, WarmBase, WarmError, WarmJob};
pub use fanout::{Delivery, FromCursor};
pub use opener::DefaultOpener;
pub use registry::{LineageKey, LineageRegistry};
pub use serve::{
    CancelResponse, RetractResponse, SendRequest, SendResponse, ServeError, ServeHandler,
    SessionImportRequest, SessionImportResponse, SessionReadLineageState, SessionReadMessage,
    SessionReadRequest, SessionReadResponse, StatusResponse, SubscribeFrom,
};

pub mod cut_state;
mod keep_warm;
pub mod move_files;
pub mod move_install;
