//! Return freed heap pages to the operating system after a memory peak.
//!
//! The allocator keeps pages it has already handed out, even after the program frees every
//! allocation on them: a burst of concurrent runs (a restart resuming dozens of long
//! conversations, say) leaves the process's resident footprint near its peak long after its
//! live heap has fallen back. Asking the allocator to give those pages back costs a walk of its
//! free lists, so it runs from an idle hook, never more often than [`MIN_INTERVAL`], and only
//! after something has been freed since the last time.
#![deny(unsafe_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The shortest time between two releases.
pub const MIN_INTERVAL: Duration = Duration::from_secs(60);

const NEVER: u64 = u64::MAX;

/// Decides when a release may run: only after [`ReliefGate::note_freed`] and never within
/// `min_interval` of the previous release.
pub struct ReliefGate {
    min_interval_ms: u64,
    last_release_ms: AtomicU64,
    freed_since_release: AtomicBool,
}

impl ReliefGate {
    pub const fn new(min_interval: Duration) -> Self {
        Self {
            min_interval_ms: min_interval.as_millis() as u64,
            last_release_ms: AtomicU64::new(NEVER),
            freed_since_release: AtomicBool::new(false),
        }
    }

    /// Record that a large owner (a finished run, a closed session) was just freed.
    pub fn note_freed(&self) {
        self.freed_since_release.store(true, Ordering::Release);
    }

    /// Claim the next release at `now_ms` (milliseconds on any monotonic clock). True means the
    /// caller must release now; the claim is recorded, so concurrent callers get at most one.
    pub fn claim_due(&self, now_ms: u64) -> bool {
        if !self.freed_since_release.load(Ordering::Acquire) {
            return false;
        }
        let last = self.last_release_ms.load(Ordering::Acquire);
        if last != NEVER && now_ms.saturating_sub(last) < self.min_interval_ms {
            return false;
        }
        if self
            .last_release_ms
            .compare_exchange(last, now_ms, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        // A free noted after this point but before the release below is still covered by that
        // release, which runs after it.
        self.freed_since_release.store(false, Ordering::Release);
        true
    }
}

static GATE: ReliefGate = ReliefGate::new(MIN_INTERVAL);

/// Record, process-wide, that memory was just freed; the next due idle tick releases it.
pub fn note_freed() {
    GATE.note_freed();
}

/// Milliseconds since the first call in this process, on a monotonic clock.
fn process_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// The idle hook: release freed pages when something was freed and the last release is at
/// least [`MIN_INTERVAL`] ago. Blocking; run it off the async workers. Returns `None` when no
/// release was due.
pub fn release_if_due() -> Option<Released> {
    GATE.claim_due(process_ms()).then(release_freed_memory)
}

/// What one release reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Released {
    /// The allocator returned this many bytes (macOS reports a count).
    Bytes(usize),
    /// The allocator reported whether it returned anything, without a count (glibc).
    Some(bool),
    /// This platform's allocator has no release call wired here.
    Unsupported,
}

/// Ask the system allocator to return free pages now, ignoring the rate limit. Blocking.
pub fn release_freed_memory() -> Released {
    sys::release()
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
mod sys {
    use std::ffi::c_void;

    extern "C" {
        // libSystem's malloc: with a null zone it walks every zone, and a goal of 0 asks it to
        // release as much as it can. It returns the number of bytes released. Measured on macOS
        // 27.0.1 (arm64) it returned 0 and left the physical footprint unchanged after a
        // 256 MiB peak was freed, so there it is a cheap no-op; the call stays for releases
        // whose allocator implements it, and the count it returns shows which case applies.
        fn malloc_zone_pressure_relief(zone: *mut c_void, goal: usize) -> usize;
    }

    pub(super) fn release() -> super::Released {
        // SAFETY: a null zone is documented to mean all zones; the call takes no other pointer.
        super::Released::Bytes(unsafe { malloc_zone_pressure_relief(std::ptr::null_mut(), 0) })
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(unsafe_code)]
mod sys {
    use std::ffi::c_int;

    extern "C" {
        // glibc: trims the top of the main heap and returns whole free pages of every arena
        // with madvise. A pad of 0 keeps nothing in reserve. Returns 1 when memory was released.
        fn malloc_trim(pad: usize) -> c_int;
    }

    pub(super) fn release() -> super::Released {
        // SAFETY: malloc_trim takes no pointer and is safe to call from any thread.
        super::Released::Some(unsafe { malloc_trim(0) } != 0)
    }
}

#[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
mod sys {
    pub(super) fn release() -> super::Released {
        super::Released::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_freed_means_no_release() {
        let gate = ReliefGate::new(MIN_INTERVAL);
        assert!(!gate.claim_due(0));
        assert!(!gate.claim_due(10 * 60_000));
    }

    #[test]
    fn releases_at_most_once_per_interval() {
        let gate = ReliefGate::new(Duration::from_secs(60));
        gate.note_freed();
        assert!(gate.claim_due(1_000), "the first free is released at once");
        // Freed again straight away: the release waits for the interval to pass.
        gate.note_freed();
        assert!(!gate.claim_due(1_001));
        assert!(!gate.claim_due(60_999));
        assert!(
            gate.claim_due(61_000),
            "due exactly one interval after the last release"
        );
        // The claim consumed the free, so a later tick with nothing new freed does nothing.
        assert!(!gate.claim_due(200_000));
    }

    #[test]
    fn concurrent_claims_release_once() {
        let gate = std::sync::Arc::new(ReliefGate::new(MIN_INTERVAL));
        gate.note_freed();
        let claims: usize = (0..8)
            .map(|_| {
                let gate = gate.clone();
                std::thread::spawn(move || gate.claim_due(5_000))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| usize::from(handle.join().unwrap()))
            .sum();
        assert_eq!(claims, 1);
    }

    #[test]
    fn release_call_reaches_the_platform_allocator() {
        let released = release_freed_memory();
        #[cfg(any(target_os = "macos", all(target_os = "linux", target_env = "gnu")))]
        assert_ne!(released, Released::Unsupported);
        #[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
        assert_eq!(released, Released::Unsupported);
    }
}
