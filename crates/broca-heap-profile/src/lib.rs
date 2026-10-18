//! Linux-only diagnostic allocator. Not linked into ordinary daemon builds.
//!
//! Requested allocation sizes are counted, not malloc arenas or RSS. Every allocation
//! carries a site id so frees on another thread are attributed to the original stack.
//! Profiler bookkeeping and its allocation headers are excluded from reported bytes.

#[cfg(target_os = "linux")]
mod linux;

/// Watch `BROCA_HEAP_PROFILE_DIR` for `<label>.request` files and atomically publish
/// `<label>.json` snapshots. The daemon need not shut down to expose its live heap.
pub fn start() {
    #[cfg(target_os = "linux")]
    linux::start();
    #[cfg(not(target_os = "linux"))]
    panic!("heap-profile requires Linux; omit the feature for a production build");
}
