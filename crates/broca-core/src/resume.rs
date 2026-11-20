//! What a resume writes before it does anything else.
//!
//! A resume picks up an episode that has no durable `RunFinished`: the process that ran it
//! crashed or was stopped. Before the resume calls a provider, dispatches a tool or seals the
//! episode, it writes one informational [`WalRecord::ResumeStarted`] saying what it found.
//!
//! When the episode holds an indeterminate tool call (a durable dispatch intent with no
//! result, so the call may or may not have run), the resume never sends it again and makes no
//! model call. It closes the open batch instead: a `ToolResult` with outcome `Unknown` for each
//! indeterminate call, an `Interrupted` result for each planned call that never left the
//! process, and the batch's commit markers. Those records go in one barrier with the resume
//! record, and the caller then seals the episode `Interrupted` in a barrier of its own. The
//! owner's next send renders every call of that batch paired with its result, so no request
//! ever carries a tool call with no result, and the model learns the outcome is uncertain
//! before it would repeat the call.
//!
//! Both the run loop and the module's crash-orphan pass resume through these functions, so the
//! close-out has one shape however the resume is reached.

pub use crate::seal::{crash_orphan_error, SealCause};
pub use crate::tool_results::{not_dispatched_result, provider_held_close_result};
pub use crate::tool_results::{NOT_DISPATCHED_TEXT, OUTCOME_UNKNOWN_TEXT};

use broca_wal::WithdrawElicitation;

/// How a sealing caller closes an unanswered Prepared call. Reader builds choose the ungated
/// not-dispatched result; writer builds supply the question's durable filing identity.
pub enum DeferredCloseKind {
    NotDispatched,
    WithdrawElicitation(WithdrawElicitation),
}
