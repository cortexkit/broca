//! All of this crate's integration tests build into this single test binary. Each former
//! `tests/<name>.rs` is the module `<name>` here, so its tests are named `<name>::<test>` (run one
//! file's tests with `cargo test -p broca-module --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.

mod common;
mod compaction;

mod coordinator_races;
mod fanout_barrier;
mod queue_render_transparency;
mod serve_dispatch;
mod session_read;
#[cfg(feature = "test-clock")]
mod test_clock;

mod open_failed_classes;
