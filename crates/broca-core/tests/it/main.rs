//! All of this crate's integration tests that can share one process build into this single test
//! binary. Each former `tests/<name>.rs` is the module `<name>` here, so its tests are named
//! `<name>::<test>` (run one file's tests with `cargo test -p broca-core --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! `input_allocation` and `capture_allocation` keep their own binaries: their global allocators
//! measure different scopes (retained size versus temporary allocation calls), and a binary
//! can install only one allocator.

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
// The read-only corpus scan example, compiled here so its tests run with this binary. Its
// `main` is unused in this build.
#[allow(dead_code)]
#[path = "../../examples/replay-corpus-scan.rs"]
mod replay_corpus_scan;
mod resume_exec;
mod run_loop;
mod run_loop_terminal_paths;
mod runner_tags;
mod runner_tags_determinism;
mod runner_tags_v1_pin;
mod session_import;
mod terminal_reason_crash_cut;
mod terminal_reason_stop_conditions;
mod wal_corruption;
