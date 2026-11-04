//! A log's checkpoint: the state its records fold onto, carried in the log's first batch.
//!
//! A session's history is split across logs: each seal or revert starts a new open log, and the
//! records before it are no longer read to load the session. So every log starts with the state
//! those earlier records folded to. Loading a session starts both folds from its checkpoint and
//! applies the log's records on top. The result must equal folding every record from the
//! session's start, which the tests check by splitting real and synthetic histories at every
//! point.
//!
//! The checkpoint is the authority for open work (approvals, held calls, open batches) carried
//! across a log boundary, not a cache: nothing else holds it once the earlier log is sealed.

use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;

crate::message! {
    /// The body of a log's checkpoint batch.
    #[derive(Debug, Clone)]
    pub struct Checkpoint {
        /// The highest seq folded into `operational`.
        1 req covers_through: u64,
        /// The seq the log's first record takes, which is above every seq the session has used.
        2 req next_seq: u64,
        /// The conversation state at the log's first record.
        3 req conversation: Conversation,
        /// The operational state as of `covers_through`.
        4 req operational: Operations,
        /// The seq of the last transcript record before the log, which the log's first
        /// transcript record names as its back-link.
        5 opt transcript_base: u64,
        /// The same for the ledger.
        6 opt ledger_base: u64,
    }
}

#[cfg(test)]
mod tests;
