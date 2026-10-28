//! Version 4 broca WAL framing, independent of broca's record types.
//!
//! This codec neither interprets content nor acquires leases. A caller must stop
//! reading a log on a header error or a newer-writer refusal. Only a verified
//! header makes it safe to advance past a damaged payload.

#![forbid(unsafe_code)]

pub mod error;
pub mod features;
pub mod frame;
pub mod proof;

pub use error::{Error, HeaderCorruption};

/// Open a log without locks or write access, allowing its owner to unlink it.
///
/// Keep share-delete enabled on Windows as well: a billing reader must never
/// prevent broca from removing an eligible log.
pub fn open_log(path: impl AsRef<std::path::Path>) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_SHARE_READ: u32 = 1;
        const FILE_SHARE_WRITE: u32 = 2;
        const FILE_SHARE_DELETE: u32 = 4;
        options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
    }
    options.open(path)
}
