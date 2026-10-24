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
mod compaction_watch;
pub mod executor;
pub mod fanout;
pub mod late_results_puller;
pub mod late_results_store;
pub mod opener;
pub mod registry;
pub mod retention;
pub mod retention_withdrawals;
mod send_fate;
pub mod serve;
pub mod session_class;

pub use cache::{CacheDeclaration, CacheDeclarer};
pub use executor::{RunExecutor, RunJob, RunKind, SealedRun, WarmBase, WarmError, WarmJob};
pub use fanout::{Delivery, FromCursor};
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
