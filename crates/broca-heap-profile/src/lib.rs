//! A sampling heap profiler for finding what a running daemon's heap holds. It is linked only
//! into builds made with `ck-broca`'s `heap-profile` feature, never into an ordinary release.
//!
//! The global allocator gives every block a small header. About one allocation per
//! [`SAMPLE_BYTES`] allocated is sampled: its call stack is captured, and it stands for the
//! bytes an unbiased estimate gives it, so each stack's live total estimates what that call
//! site holds now. A sampled block's free takes its estimate back out, whichever thread frees
//! it. Requested sizes are counted, not malloc's size classes, fragmentation or RSS, and the
//! profiler's own bookkeeping is left out.
//!
//! Snapshots are asked for by file: [`start`] watches a folder for `<label>.request` and answers
//! with `<label>.json`, so a profile can be read from the daemon while it serves. Each snapshot
//! names the executable's load address, so its stacks can be symbolized afterwards with the
//! same binary (`atos -o ck-broca -l <load address>` on macOS, `addr2line` on Linux).

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod sampled;

/// On average, one allocation is sampled per this many bytes allocated. Smaller is more exact
/// and slower; at 512 KiB a daemon allocating 100 MB a second captures about 200 stacks a second.
pub const SAMPLE_BYTES: u64 = 512 * 1024;

/// Watches `dir` for `<label>.request` files and publishes `<label>.json` snapshots of the
/// sampled heap. Sampling runs from the first allocation whether or not this is called.
pub fn start(dir: std::path::PathBuf) {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    sampled::start(dir);
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    panic!(
        "heap-profile runs on Linux and macOS; omit the feature here (asked for {})",
        dir.display()
    );
}
