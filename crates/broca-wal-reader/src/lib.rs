//! Version 4 broca WAL framing, independent of broca's record types.
//!
//! This codec neither interprets content nor acquires leases. A caller must stop
//! reading a log on a header error or a newer-writer refusal. Only a verified
//! header makes it safe to advance past a damaged payload.
//!
//! Durability is a claim by the trusted writer/filesystem, not an authenticated
//! property of a cyclic redundancy check (CRC) or digest.

#![forbid(unsafe_code)]

pub mod error;
pub mod features;
pub mod frame;
pub mod vault_facts;

pub use error::{Error, HeaderCorruption};
