//! All of this crate's integration tests build into this single test binary. Each former
//! `tests/<name>.rs` is the module `<name>` here, so its tests are named `<name>::<test>` (run one
//! file's tests with `cargo test -p ck-import --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! The real-daemon rig in `real_module_e2e` removes `SUBC_MODULE_ID` and `SUBC_LAUNCH_NONCE` from this
//! process's own environment. That is safe to share: the removal only ever moves toward the state
//! every test here wants, and nothing restores the variables.

mod cli;
mod codex_parallel_import;
mod real_module_e2e;
