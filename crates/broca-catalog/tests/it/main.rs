//! All of this crate's integration tests that can share one process build into this single test
//! binary. Each former `tests/<name>.rs` is the module `<name>` here, so its tests are named
//! `<name>::<test>` (run one file's tests with `cargo test -p broca-catalog --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! Some files keep binaries of their own:
//! - `code_assist_variant_resume`, `default_max_output`, `reasoning_variants` and `service_tier`
//!   each install a different fixture as the process-global catalog and resolve through it, so any
//!   two of them in one process would read each other's catalog. Only `family_override_resolve`
//!   installs a global catalog here, and nothing else here reads one.
//! - `tests/live`: the live suite, which calls real provider APIs; the Rust gate excludes it by
//!   binary name.

#[cfg(all(feature = "bundled-catalog", feature = "live"))]
#[path = "../common/mod.rs"]
mod common;

mod base_url_pins;
mod family_override_resolve;
mod fusiform_wire_fixture;
mod live_fusiform_payload;
mod resolution_report;
mod wire_family_pins;
