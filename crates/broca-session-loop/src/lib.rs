//! The run loop for sessions on broca's session store (`broca-session-store`).
//!
//! A run is a sequence of model steps and tool batches. The loop reads the state the store
//! keeps current: the conversation fold (what the model sees) and the operational fold
//! (where the run stands). Every effect is a record appended through one [`Writer`], so the
//! loop and the session actor, which appends steers and queued sends while a run is active,
//! always read the same state.
//!
//! The durability rules are:
//! - a run's start and input are durable before its first model call;
//! - a batch's tool-call intents are durable before any call is dispatched;
//! - a batch's results are durable before the next model call;
//! - a call that may have run is never sent again.
//!
//! Model steps use [`broca_core::Provider`], and tool calls use
//! [`broca_core::tool_plane::ToolPlane`].
//!
//! A session uses the layout its first run started under: the established layout, or the
//! current compaction contract when its plan names a compaction provider. Under the
//! contract, system text is run setup, not transcript; every message gets its entry facts
//! when placed (see [`entries`]), and requests render from those entries.
//!
//! A run covers text steps and tool batches, stop conditions, provider errors, steers and
//! ordered deliveries, cancels, pauses, hooks, the approval policy, calls a provider holds,
//! repetition guidance, media in tool results, compaction views and tool-call budgets. It takes
//! no approval questions and no input-token budget.

// The memory budget tests count what their thread allocates (see `broca_test_support::alloc`).
// Only the test binary installs the counting allocator.
#[cfg(test)]
#[global_allocator]
static ALLOCATOR: broca_test_support::alloc::CountingAlloc =
    broca_test_support::alloc::CountingAlloc;

pub mod account_move;
pub mod admit;
pub mod billing;
pub mod budget;
pub mod compaction;
pub mod durable;
pub mod enroll;
pub mod entries;
pub mod media;
pub mod media_index;
pub mod output;
pub mod owner;
pub mod parts;
pub mod plan;
pub mod plan_tools;
pub mod queue;
mod refusal;
mod repetition;
pub mod run;
pub mod shared;
pub mod today;
mod transport;
mod window_shape;
pub mod writer;

pub use media::{HeldInputMedia, PendingMedia};
pub use parts::{MemoryParts, Parts};
pub use run::{resume, run, seal, AccountReports, Admission, Finished, LoopError, Seams};
pub use writer::Writer;

#[cfg(test)]
mod early_result_tests;
#[cfg(test)]
mod shared_text_tests;
#[cfg(test)]
mod step_budget_tests;
#[cfg(test)]
mod step_limit_tests;
#[cfg(test)]
mod tool_versions_tests;
