//! All of this crate's integration tests build into this single test binary. Each file in
//! `tests/it/` is a module here, so its tests are named `<file>::<test>` (run one file's tests
//! with `cargo test -p broca-module --test it <file>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.

mod open_failed_classes;
