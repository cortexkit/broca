//! Making written bytes durable: one call for every file and folder the store syncs.
//!
//! On macOS `File::sync_all` issues `F_FULLFSYNC`, which also makes the drive empty its write
//! cache. That costs about 4 ms on Apple's SSDs, measured at 4.8 ms per call on this store's
//! own writes, and a writer open, a head write and every step paid for several. A plain
//! `fsync(2)` hands the bytes to the drive and returns in tens of microseconds. What it gives up
//! is limited to a power loss or kernel panic before the drive writes its cache out: the newest
//! writes can then be missing, and a tool call recorded just before can run again when its run
//! resumes. A broca crash, kill or restart loses nothing, because the kernel already holds the
//! bytes. Most of these machines run on batteries, where power loss isn't the risk, so the plain
//! `fsync` is the default and the full flush is the operator's setting
//! ([`configure_full_flush`]). Elsewhere `sync_all` is the platform's own full sync (`fsync` on
//! Linux, `FlushFileBuffers` on Windows), and is kept either way.

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Whether a sync on macOS also flushes the drive's write cache (`F_FULLFSYNC`).
static FULL_FLUSH: AtomicBool = AtomicBool::new(false);

/// Makes every later sync on macOS a full flush of the drive's write cache, or a plain `fsync`.
/// The module sets it once at startup, from its configuration, before any session opens.
pub fn configure_full_flush(full: bool) {
    FULL_FLUSH.store(full, Ordering::Relaxed);
}

/// Makes what was written to `file` durable (see the module's note on macOS).
pub fn sync(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    if !FULL_FLUSH.load(Ordering::Relaxed) {
        return rustix::fs::fsync(file).map_err(io::Error::from);
    }
    file.sync_all()
}

/// Makes the entries of folder `path` (names created, renamed or removed in it) durable. Windows
/// has no folder sync; a rename there is durable once it returns.
pub fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    sync(&File::open(path)?)?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
