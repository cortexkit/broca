//! Shared by both sweep arms: set a DIRECTORY's modification time, on every platform.
//!
//! `File::open` on a directory is a unix-ism. Windows refuses it with
//! `PermissionDenied ("Access is denied.")` unless the handle is opened with
//! FILE_FLAG_BACKUP_SEMANTICS, which is what `std::os::windows::fs::OpenOptionsExt::custom_flags`
//! exists for. CI caught exactly that: both arms passed locally on macOS and the reclaim arm
//! failed on Windows with `open planted dir: Os { code: 5, kind: PermissionDenied }`.
//!
//! This lives in one place because the two arms would otherwise each carry the same platform
//! branch, and a fix applied to one of them would leave the other broken on the platform that
//! is hardest to notice.
//!
//! It is `tests/age/mod.rs` rather than `tests/age.rs` because cargo builds every top-level file
//! in `tests/` as a test binary of its own, and this one would hold no tests.
#![allow(dead_code)]

use std::path::Path;
use std::time::SystemTime;

/// Sets `dir`'s modification time. Panics with the path on failure, since a test that could not
/// plant its fixture must not continue and report a passing sweep.
pub fn set_dir_mtime(dir: &Path, when: SystemTime) {
    #[cfg(windows)]
    let handle = {
        use std::os::windows::fs::OpenOptionsExt;
        // 0x0200_0000 = FILE_FLAG_BACKUP_SEMANTICS, required to obtain a directory handle.
        std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(0x0200_0000)
            .open(dir)
    };
    #[cfg(not(windows))]
    let handle = std::fs::File::open(dir);

    handle
        .unwrap_or_else(|error| panic!("open planted dir {}: {error}", dir.display()))
        .set_modified(when)
        .unwrap_or_else(|error| panic!("age planted dir {}: {error}", dir.display()));
}
