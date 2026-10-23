//! A one-time lifecycle log line for a compaction session whose stored transcript has grown
//! past an operational watch threshold.
//!
//! Until history starts at the compaction cut, a run of such a session holds the session's
//! whole stored transcript in memory, and a resume reads it from message 0, whatever the
//! compaction cut. Compaction keeps working past the threshold; the line only tells an
//! operator which sessions have reached the size where that cost shows. A session can enroll
//! in compaction only on its first episode, when it has no stored messages, so nothing has to
//! be refused at enrollment: the threshold matters only for sessions that grow afterwards.

/// The stored-message count past which a session running under a compaction plan is logged
/// once. A temporary operational watch: delete it, with [`CompactionSessionWatch`] and its
/// log line, in the release where history starts at the compaction cut and broca's
/// 150,000-message session benchmark passes with it.
pub const COMPACTION_SESSION_WATCH_MESSAGES: u64 = 20_000;

/// Whether this actor has already logged its session passing
/// [`COMPACTION_SESSION_WATCH_MESSAGES`]. Kept per actor, so the line appears at most once per
/// actor lifetime.
#[derive(Debug, Default)]
pub(crate) struct CompactionSessionWatch {
    logged: bool,
}

impl CompactionSessionWatch {
    /// Whether the session still needs its stored count read: false once the line was logged.
    pub(crate) fn pending(&self) -> bool {
        !self.logged
    }

    /// Record a stored-message count. Returns true exactly once: for the first count past the
    /// threshold, which the caller then logs.
    pub(crate) fn observe(&mut self, stored_messages: u64) -> bool {
        if self.logged || stored_messages <= COMPACTION_SESSION_WATCH_MESSAGES {
            return false;
        }
        self.logged = true;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crossing_the_threshold_is_reported_once() {
        let mut watch = CompactionSessionWatch::default();
        assert!(watch.pending());
        assert!(!watch.observe(0));
        assert!(!watch.observe(COMPACTION_SESSION_WATCH_MESSAGES));
        assert!(watch.pending(), "reaching the threshold is not passing it");
        assert!(watch.observe(COMPACTION_SESSION_WATCH_MESSAGES + 1));
        assert!(!watch.pending());
        assert!(!watch.observe(COMPACTION_SESSION_WATCH_MESSAGES + 2));
        assert!(!watch.observe(150_000));
    }
}
