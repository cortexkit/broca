//! Test-only SQLite scheduling support; no shipped crate depends on this package.
#![cfg(test)]

// Compile the production policy and timeout verbatim, substituting only its test
// sleep hook for wall-clock delays. Keep VFS FFI outside the production library
// so its unsafe-code prohibition remains intact.
#[path = "../../broca-store/src/busy.rs"]
mod busy;
mod contention;
mod vfs;

// Only the connection field is needed by the production ref-index implementation.
// Compile the production session-ref index implementation directly so this
// harness exercises changes to its busy setup, not a test-local reconstruction.
struct SqliteStore {
    conn: std::sync::Mutex<rusqlite::Connection>,
}
type StoreError = Box<dyn std::error::Error + Send + Sync>;
const STORE_BUSY_TIMEOUT: std::time::Duration = busy::STORE_BUSY_TIMEOUT;
#[path = "../../broca-store/src/session_refs/write.rs"]
mod ref_write;
