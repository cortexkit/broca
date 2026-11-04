//! Folds: rebuilding a session's state by reading its records in order. Each stream has its own
//! fold, and a fold reads only its own stream's records, so the conversation can be rebuilt from
//! the transcript alone and the operational state from the ledger alone.

pub mod conversation;
pub mod operations;
