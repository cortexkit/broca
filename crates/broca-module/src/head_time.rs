//! When a session on today's storage last changed, as `session.head` serves it (`updated_at`).
//! The head and a move into the session store both take it from here: a move records it in the
//! moved session (`Record::Moved`), so the head reads the same after the move.

use std::time::SystemTime;

/// `time` in milliseconds since the Unix epoch; `None` before it.
pub fn epoch_ms(time: SystemTime) -> Option<u64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
}
