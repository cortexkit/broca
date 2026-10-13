//! This crate's integration tests that can share one process build into this single test binary.
//! Each former `tests/<name>.rs` is the module `<name>` here, so its tests are named
//! `<name>::<test>` (run one file's tests with `cargo test -p broca-crash-harness --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! `ported_scenarios`, `resume_close_out` and `steer_interrupt` keep binaries of their own. Their
//! process kills re-run the current test binary with `--exact <child test>`, naming the child entry
//! by its top-level test name (`broca_crash_harness::CHILD_TEST_NAME`, or `steer_kill_child`); as a
//! module of this binary that test would be named `<file>::<test>`, and the re-run would select
//! nothing.

#[path = "../scenarios/mod.rs"]
mod scenarios;

mod graceful_stop;
mod hand_written_expectations;
mod plan_manifest_redrive;
mod scope_migration;
mod script_fixtures;
