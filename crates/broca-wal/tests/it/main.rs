//! All of this crate's integration tests build into this single test binary. Each former
//! `tests/<name>.rs` is the module `<name>` here, so its tests are named `<name>::<test>` (run one
//! file's tests with `cargo test -p broca-wal --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.

// Fixtures deliberately prepare and inspect files outside the production seam.
#![allow(clippy::disallowed_methods)]

mod archive_inventory;
mod archive_reader_perf;
mod compaction_reader;
mod compaction_source;
mod historical_usage;
mod retention_bytes;
mod retention_source;
