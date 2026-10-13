//! The live suite: these tests resolve real catalog rows and call real provider APIs, so they run
//! only by hand and the Rust gate excludes this binary by name. Each former `tests/<name>.rs` is the
//! module `<name>` here; run one file with
//! `cargo test -p broca-catalog -F live --test live <name>:: -- --nocapture`.
//!
//! Both files install the same vendored snapshot as the process-global catalog, so they can share a
//! process. It is one binary rather than one per file for the same reason as the crate's main `it`
//! binary: every test executable is linked separately and, on macOS, pays a code-signing check on its
//! first run.

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
#[path = "../common/mod.rs"]
mod common;

mod catalog_live_e2e;
mod catalog_native_live_e2e;
