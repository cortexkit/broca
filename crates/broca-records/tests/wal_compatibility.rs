//! Pins the shared records' bytes through the old log format's frame codec, which broca-wal
//! still carries: the record vocabulary and its feature names are live even though the old log
//! store is gone.

pub use broca_records::{recovery, WalRecord};
pub use broca_wal::framing;

#[path = "../src/record/wal_tests.rs"]
mod record;
