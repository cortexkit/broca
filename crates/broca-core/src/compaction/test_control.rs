//! Debug-only process controls for real-daemon compaction conformance.
//! The release build cannot link this module or its filesystem ingress.

use std::path::{Path, PathBuf};

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

impl Config {
    pub(crate) fn load(
        clock: Option<&crate::Clock>,
        session: &str,
    ) -> Result<Option<Self>, crate::RunError> {
        let Some(root) = clock.and_then(crate::Clock::test_root) else {
            return Ok(None);
        };
        let root = root.join("test-compaction").join(session);
        match std::fs::read(root.join("config")) {
            Ok(bytes) => {
                let mut config: Self = serde_json::from_slice(&bytes).map_err(|e| {
                    crate::RunError::Wal(format!("invalid test compaction control: {e}"))
                })?;
                config.root = root;
                Ok(Some(config))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(crate::RunError::Wal(e.to_string())),
        }
    }
}

pub(crate) async fn barrier(
    clock: Option<&crate::Clock>,
    session: &str,
    point: &str,
) -> Result<(), crate::RunError> {
    let Some(root) = clock.and_then(crate::Clock::test_root) else {
        return Ok(());
    };
    let gate = root.join("test-compaction-gate");
    if std::fs::read_to_string(&gate).ok().as_deref() != Some(point) {
        return Ok(());
    }
    publish(
        &root.join("test-compaction-reached"),
        &serde_json::to_vec(&serde_json::json!({"point":point,"session":session})).unwrap(),
    )?;
    // The gate file names a durable checkpoint where execution must pause. Publish
    // arrival before polling model transport so SIGKILL cannot occur after model receipt.
    // Removing the file releases the pause; the harness removes it before process restart
    // so the new process cannot inherit the previous process's armed checkpoint.
    while gate.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    Ok(())
}

/// Write a file the harness polls for. A plain write creates the file empty before filling
/// it, so a reader polling every few milliseconds can see zero bytes; writing a temporary
/// file and renaming it over the final name makes the content appear all at once.
fn publish(path: &Path, bytes: &[u8]) -> Result<(), crate::RunError> {
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temp, bytes)
        .and_then(|()| std::fs::rename(&temp, path))
        .map_err(|e| crate::RunError::Wal(e.to_string()))
}
