//! Debug-only process controls for real-daemon compaction conformance.
//! The release build cannot link this module or its filesystem ingress.

use std::path::PathBuf;

/// The harness's settings for one session. Members the harness writes that this build does
/// not use are ignored.
#[derive(Clone, serde::Deserialize)]
pub struct Config {
    pub call_timeout_ms: u64,
    pub context_window: u64,
    pub output_limit: u64,
    #[serde(skip)]
    pub root: PathBuf,
}

impl Config {}
