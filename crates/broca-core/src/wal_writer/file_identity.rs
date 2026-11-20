//! Whether two paths name the same log file, by the file system's own identity rather than by
//! path, so a hard link left by an interrupted placement counts as the same file.

use std::path::Path;

use super::WalError;

#[cfg(unix)]
pub(super) fn same_wal_file(left: &Path, right: &Path) -> Result<bool, WalError> {
    use std::os::unix::fs::MetadataExt;
    let left = broca_wal::fs_ops::metadata(left).map_err(WalError::Io)?;
    let right = broca_wal::fs_ops::metadata(right).map_err(WalError::Io)?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(windows)]
pub(super) fn same_wal_file(left: &Path, right: &Path) -> Result<bool, WalError> {
    let left = broca_wal::fs_ops::File::open(left).map_err(WalError::Io)?;
    let right = broca_wal::fs_ops::File::open(right).map_err(WalError::Io)?;
    // Placement repair may remove an alias, so compare the complete handle identities,
    // not the lossy hashes used to invalidate read caches.
    Ok(same_native_file_identity(
        left.native_file_identity(),
        right.native_file_identity(),
    ))
}

#[cfg(any(windows, test))]
pub(super) fn same_native_file_identity(
    left: Option<(u32, u64)>,
    right: Option<(u32, u64)>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

#[cfg(not(any(unix, windows)))]
pub(super) fn same_wal_file(_left: &Path, _right: &Path) -> Result<bool, WalError> {
    // Never guess whether paths are aliases when native file identity is unavailable.
    Ok(false)
}
