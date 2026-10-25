//! The session-length benchmark's unit tests, built as an integration test target. The
//! mutation catalogue replays only library, binary and integration test targets, not
//! examples, so its rows for the benchmark name the tests through this target.

// Only the benchmark's tests run here; its measuring code is unused in this binary.
#[allow(dead_code)]
#[path = "../examples/session_length_cost.rs"]
mod session_length_cost;
