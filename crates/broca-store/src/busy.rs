//! SQLite lock-wait policies shared with the deterministic contention harness.
use rusqlite::Connection;
use std::time::{Duration, Instant};

pub(crate) const STORE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const BUSY_POLL_INTERVAL: Duration = Duration::from_millis(1);

thread_local! {
    static BUSY_STARTED: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
    static WRITE_DEADLINE: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

// SQLite's default busy handler backs off to 100 ms. A stream of short foreground
// transactions can keep winning the lock between those polls, especially on Windows,
// even though no writer holds it for the full timeout. Poll at a fixed short interval,
// retaining the store's bounded wait rather than retrying a failed batch.
pub(crate) fn configure_writer_connection(conn: &Connection) -> rusqlite::Result<()> {
    conn.busy_handler(Some(wait_for_write_lock))
}

/// Include time spent waiting for the store mutex without replacing the connection's
/// busy handler. SQLite's busy_timeout API would silently reinstall default backoff.
pub(crate) fn with_write_deadline<T>(deadline: Instant, write: impl FnOnce() -> T) -> T {
    struct RestoreDeadline(Option<Instant>);
    impl Drop for RestoreDeadline {
        fn drop(&mut self) {
            WRITE_DEADLINE.with(|current| current.set(self.0));
        }
    }
    let _restore = RestoreDeadline(WRITE_DEADLINE.with(|current| current.replace(Some(deadline))));
    write()
}

fn busy_poll_delay(elapsed: Duration) -> Option<Duration> {
    STORE_BUSY_TIMEOUT
        .checked_sub(elapsed)
        .filter(|remaining| !remaining.is_zero())
        .map(|remaining| remaining.min(BUSY_POLL_INTERVAL))
}

fn wait_for_write_lock(attempt: i32) -> bool {
    BUSY_STARTED.with(|started| {
        // SQLite restarts the count for each lock acquisition. The handler neither
        // re-enters SQLite nor moves threads, so this clock is local to that wait.
        let start = if attempt == 0 {
            let now = Instant::now();
            started.set(Some(now));
            now
        } else {
            started.get().expect("busy wait starts at attempt zero")
        };
        let Some(delay) = busy_poll_delay(start.elapsed()) else {
            return false;
        };
        let delay = WRITE_DEADLINE.with(|deadline| {
            deadline.get().map_or(delay, |deadline| {
                delay.min(deadline.saturating_duration_since(Instant::now()))
            })
        });
        if delay.is_zero() {
            return false;
        }
        #[cfg(test)]
        BUSY_WAIT_HOOK.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook(delay);
            }
        });
        sleep_between_busy_attempts(delay);
        start.elapsed() < STORE_BUSY_TIMEOUT
            && WRITE_DEADLINE.with(|deadline| deadline.get().is_none_or(|end| Instant::now() < end))
    })
}

pub(crate) fn sleep_between_busy_attempts(delay: Duration) {
    #[cfg(test)]
    if BUSY_SLEEP_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(delay);
            true
        } else {
            false
        }
    }) {
        return;
    }
    std::thread::sleep(delay);
}

#[cfg(test)]
type BusyWaitHook = Box<dyn FnOnce(Duration)>;
#[cfg(test)]
type BusySleepHook = Box<dyn FnMut(Duration)>;

#[cfg(test)]
thread_local! {
    pub(crate) static BUSY_WAIT_HOOK: std::cell::RefCell<Option<BusyWaitHook>> = const { std::cell::RefCell::new(None) };
    pub(crate) static BUSY_SLEEP_HOOK: std::cell::RefCell<Option<BusySleepHook>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_busy_poll_interval_stays_short_until_deadline() {
        for elapsed in [
            Duration::ZERO,
            Duration::from_secs(2),
            Duration::from_secs(4),
        ] {
            assert_eq!(busy_poll_delay(elapsed), Some(Duration::from_millis(1)));
        }
        assert_eq!(
            busy_poll_delay(STORE_BUSY_TIMEOUT - Duration::from_micros(500)),
            Some(Duration::from_micros(500))
        );
        assert_eq!(busy_poll_delay(STORE_BUSY_TIMEOUT), None);
        assert_eq!(
            busy_poll_delay(STORE_BUSY_TIMEOUT + Duration::from_secs(1)),
            None
        );
    }

    #[test]
    fn migration_busy_handler_resets_and_expires_wait_budget() {
        BUSY_STARTED.with(|started| {
            started.set(Some(Instant::now() - STORE_BUSY_TIMEOUT));
        });
        assert!(!wait_for_write_lock(1), "an expired wait must stop");
        assert!(
            wait_for_write_lock(0),
            "a new acquisition gets a new budget"
        );
    }

    #[test]
    fn write_deadline_caps_lock_wait_and_restores_after_unwind() {
        let expired = Instant::now() - Duration::from_secs(1);
        with_write_deadline(expired, || {
            assert!(
                !wait_for_write_lock(0),
                "mutex wait consumes the lock budget"
            );
        });
        assert!(WRITE_DEADLINE.with(|deadline| deadline.get().is_none()));
        let outer = Instant::now() + STORE_BUSY_TIMEOUT;
        with_write_deadline(outer, || {
            let result = std::panic::catch_unwind(|| {
                with_write_deadline(expired, || panic!("failed write"));
            });
            assert!(result.is_err());
            assert_eq!(WRITE_DEADLINE.with(|deadline| deadline.get()), Some(outer));
        });
        assert!(WRITE_DEADLINE.with(|deadline| deadline.get().is_none()));
    }
}
