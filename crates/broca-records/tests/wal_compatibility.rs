//! Exercise the shared records through the legacy WAL codec and writer.

pub use broca_records::WalRecord;
pub use broca_wal::{framing, recovery, FileWal, Wal, WalError};

#[path = "../src/record/wal_tests.rs"]
mod record;

#[path = "../src/floor/wal_tests.rs"]
mod floor;
