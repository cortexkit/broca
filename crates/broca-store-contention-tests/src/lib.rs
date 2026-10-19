//! Test-only SQLite scheduling support; no shipped crate depends on this package.
#![cfg(test)]

// Compile the production policy and timeout verbatim, substituting only its test
// sleep hook for wall-clock delays. Keep VFS FFI outside the production library
// so its unsafe-code prohibition remains intact.
#[path = "../../broca-store/src/busy.rs"]
mod busy;
mod contention;
mod vfs;
