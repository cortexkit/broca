//! The run loop for sessions on broca's new session storage (`broca-session-store`).
//!
//! A run is a sequence of model steps and tool batches. Today's loop (`broca_core::run`)
//! rebuilds its state by replaying its log in several places; this loop reads the state the
//! store keeps current instead: the conversation fold (what the model sees) and the operational
//! fold (where the run stands). Every effect is a record appended through one [`Writer`], so the
//! loop and the session actor, which appends steers and queued sends while a run is active,
//! always read the same state.
//!
//! The durability rules are today's:
//! - a run's start and input are durable before its first model call;
//! - a batch's tool-call intents are durable before any call is dispatched;
//! - a batch's results are durable before the next model call;
//! - a call that may have run is never sent again.
//!
//! The seams the loop calls are today's too: [`broca_core::Provider`] for model steps and
//! [`broca_core::tool_plane::ToolPlane`] for tool calls, so every provider family and tool
//! route works unchanged.
//!
//! A session runs in the layout its first run started under, as in today's loop: the
//! established one, or the current compaction contract, which today's actor picks for a session
//! whose plan names a compaction provider. On the contract the system text is run setup, not
//! transcript, every message gets its entry facts when it's placed (see [`entries`]), and a
//! request renders from those entries as today's loop renders one.
//!
//! What this build covers: runs with text steps and tool batches, stop conditions, provider
//! errors, steers and ordered deliveries, cancels, pauses, hooks, the approval policy, calls a
//! provider holds, repetition guidance and media in tool results. It doesn't cover compaction
//! views, approval questions, budgets or the rest of today's loop yet. Each part it covers is
//! proven against today's loop by the differential tests in `tests/`.

pub mod admit;
pub mod budget;
pub mod compaction;
pub mod entries;
pub mod media;
pub mod output;
pub mod owner;
pub mod parts;
pub mod queue;
mod refusal;
pub mod run;
pub mod shared;
pub mod writer;

pub use media::PendingMedia;
pub use parts::{MemoryParts, Parts};
pub use run::{resume, run, seal, AccountReports, Admission, Finished, LoopError, Seams};
pub use writer::Writer;
