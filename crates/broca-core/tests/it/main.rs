//! All of this crate's integration tests that can share one process build into this single test
//! binary. Each former `tests/<name>.rs` is the module `<name>` here, so its tests are named
//! `<name>::<test>` (run one file's tests with `cargo test -p broca-core --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! `input_allocation` keeps a binary of its own: it installs a counting `#[global_allocator]`, which
//! would count every other test's allocations too, and a binary can have only one.

mod append_only_render;
mod cancel_watchdog;
mod graceful_stop;
mod input_token_cap;
mod lineage_replay;
mod non_compaction_request_bytes;
mod pause_continue;
mod projection;
mod queue_crash;
mod replay;
mod resume_exec;
mod run_loop;
mod run_loop_terminal_paths;
mod runner_tags;
mod runner_tags_determinism;
mod session_import;
mod terminal_reason_crash_cut;
mod terminal_reason_stop_conditions;
mod wal_corruption;
