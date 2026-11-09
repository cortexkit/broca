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
use crate::fold::Bound;

crate::fold::state! {
    /// The body of a log's checkpoint batch.
    #[derive(Debug, Clone)]
    pub struct Checkpoint {
        /// The highest seq folded into `operational`.
        1 req pub covers_through: u64 => {
            owner: "storage", reset: "ChangeIntent", bound: Bound::Fixed,
        },
        /// The seq the log's first record takes, which is above every seq the session has used.
        2 req pub next_seq: u64 => {
            owner: "storage", reset: "never", bound: Bound::Fixed,
        },
        /// The conversation state at the log's first record.
        3 req pub conversation: Conversation => {
            owner: "conversation", reset: "ChangeIntent", bound: Bound::Declared,
        },
        /// The operational state as of `covers_through`.
        4 req pub operational: Operations => {
            owner: "operations", reset: "ChangeIntent", bound: Bound::Declared,
        },
        /// The seq of the last transcript record before the log, which the log's first
        /// transcript record names as its back-link.
        5 opt pub transcript_base: u64 => {
            owner: "storage", reset: "ChangeIntent", bound: Bound::Fixed,
        },
        /// The same for the ledger.
        6 opt pub ledger_base: u64 => {
            owner: "storage", reset: "ChangeIntent", bound: Bound::Fixed,
        },
        /// Activity after every record below `next_seq`, including a move's frozen baseline.
        /// Copied transcript records below that seq have already contributed and cannot advance
        /// it again. Absent means unknown, including checkpoints written before it was stored.
        7 opt pub activity_ms: u64 => {
            owner: "session", reset: "Moved", bound: Bound::Fixed,
        },
    }
}

#[cfg(test)]
mod tests;
