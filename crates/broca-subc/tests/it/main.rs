//! All of this crate's integration tests that can share one process build into this single test
//! binary. Each former `tests/<name>.rs` is the module `<name>` here, so its tests are named
//! `<name>::<test>` (run one file's tests with `cargo test -p broca-subc --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! `live_e2e` keeps a binary of its own: it calls real provider APIs, and the Rust gate excludes it
//! by binary name.
//!
//! The rigs here remove `SUBC_MODULE_ID` and `SUBC_LAUNCH_NONCE` from this process's own
//! environment before talking to their daemon. That is safe to share: the removal only ever moves
//! toward the state every test here wants, and nothing restores the variables.

#[path = "../common/mod.rs"]
mod common;

mod aimock_real_aft;
mod compaction_routes;
mod consent_ops;
mod dogfood;
mod hook_routes;
mod multi_module_spine;
mod resume_spine;
mod scope_outbound_real_daemon;
mod session_door;
mod subc_guard;
mod tool_call_cancel_real_daemon;
mod tool_fault_spine;
