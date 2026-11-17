//! When a session on today's storage last changed, as `session.head` serves it (`updated_at`).
//! The head and a move into the new storage both take it from here: a move records it in the
//! moved session (`Record::Moved`), so the head reads the same after the move.

use std::path::Path;
use std::time::SystemTime;

/// The last update `session.head` serves from what it knows without reading a session's whole
/// WAL: a retained session's retention activity, which maintenance never moves; else when its
/// live WAL file was last written; else the newest time a record of the WAL's tail carries.
/// `None` when none of them is known, and the head then reads the whole WAL
/// ([`newest_record_ms`]), or serves 0 when no record carries a time.
pub fn known_updated_at(
    retention_activity_ms: Option<u64>,
    live_wal_written_ms: Option<u64>,
    tail_newest_ms: Option<u64>,
) -> Option<u64> {
    retention_activity_ms
        .or(live_wal_written_ms)
        .or(tail_newest_ms)
}

/// `time` in milliseconds since the Unix epoch; `None` before it.
pub fn epoch_ms(time: SystemTime) -> Option<u64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}

/// When the file at `path` was last written, in milliseconds since the Unix epoch; `None` when
/// it can't be read.
pub fn file_written_ms(path: &Path) -> Option<u64> {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(epoch_ms)
}
