//! Shared clock domain for durable timestamps, retention decisions and actor timers.

#[cfg(all(feature = "test-clock", not(debug_assertions)))]
compile_error!(
    "test-clock cannot be enabled in a release build; it can move retention deletion deadlines"
);

/// Release staging refuses this literal whenever the filesystem-controlled clock is linked.
#[cfg(feature = "test-clock")]
pub const TEST_CLOCK_BINARY_MARKER: &str = "BROCA_TEST_CLOCK_RETENTION_CLOCK_OVERRIDE_V1";

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

/// Clones share the same offset and change notifications. Production builds always
/// use the real clock and never read the test control file.
#[derive(Clone, Debug)]
pub struct Clock {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    changes: watch::Sender<i64>,
    #[cfg(feature = "test-clock")]
    control: Option<std::path::PathBuf>,
    #[cfg(feature = "test-clock")]
    fixed: std::sync::Mutex<Option<u64>>,
}

impl Default for Clock {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                changes: watch::channel(0).0,
                #[cfg(feature = "test-clock")]
                control: None,
                #[cfg(feature = "test-clock")]
                fixed: std::sync::Mutex::new(None),
            }),
        }
    }
}

impl Clock {
    #[cfg(feature = "test-clock")]
    pub(crate) fn test_root(&self) -> Option<std::path::PathBuf> {
        self.inner
            .control
            .as_ref()?
            .parent()
            .map(std::path::Path::to_path_buf)
    }
    /// Construct the daemon's clock. The module owns its lifetime and injects
    /// clones into writers, actors and the sweep.
    pub fn for_state_root(root: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        #[cfg(feature = "test-clock")]
        {
            let clock = Self {
                inner: Arc::new(Inner {
                    changes: watch::channel(0).0,
                    control: Some(root.as_ref().join("test-clock")),
                    fixed: std::sync::Mutex::new(None),
                }),
            };
            clock.refresh()?;
            tracing::info!(
                offset_ms = *clock.inner.changes.borrow(),
                "{TEST_CLOCK_BINARY_MARKER}: test clock offset at boot"
            );
            Ok(clock)
        }
        #[cfg(not(feature = "test-clock"))]
        {
            let _ = root;
            Ok(Self::default())
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.read_now_ms()
    }

    fn read_now_ms(&self) -> u64 {
        #[cfg(feature = "test-clock")]
        if let Some(now) = *self.inner.fixed.lock().expect("clock control lock") {
            return now;
        }
        let real = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            });
        real.saturating_add_signed(*self.inner.changes.borrow())
    }

    /// Timers and the sweep must recompute their deadlines when this watch changes.
    pub fn subscribe(&self) -> watch::Receiver<i64> {
        self.inner.changes.subscribe()
    }

    /// Re-read before a sweep decision as well as on the short polling interval.
    /// Invalid controls fail explicitly rather than silently changing the time domain.
    pub fn refresh(&self) -> std::io::Result<bool> {
        #[cfg(feature = "test-clock")]
        {
            self.refresh_with_reader(|path| std::fs::read(path))
        }
        #[cfg(not(feature = "test-clock"))]
        {
            Ok(false)
        }
    }

    #[cfg(feature = "test-clock")]
    fn refresh_with_reader(
        &self,
        read: impl FnOnce(&std::path::Path) -> std::io::Result<Vec<u8>>,
    ) -> std::io::Result<bool> {
        let Some(path) = &self.inner.control else {
            return Ok(false);
        };
        #[derive(serde::Deserialize)]
        struct Control {
            #[serde(default)]
            offset_ms: i64,
            #[serde(default)]
            now_ms: Option<u64>,
        }
        // Refresh also runs on admission, concurrently with the filesystem poller.
        // Serialize the read with its application so an older read cannot restore
        // real time after another refresh has installed the fixed test clock.
        let mut fixed = self.inner.fixed.lock().expect("clock control lock");
        let control = match read(path) {
            Ok(bytes) => serde_json::from_slice::<Control>(&bytes)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Control {
                offset_ms: 0,
                now_ms: None,
            },
            Err(error) => return Err(error),
        };
        let fixed_changed = {
            let changed = *fixed != control.now_ms;
            *fixed = control.now_ms;
            changed
        };
        Ok(self.inner.changes.send_if_modified(|current| {
            if *current == control.offset_ms && !fixed_changed {
                return false;
            }
            *current = control.offset_ms;
            true
        }))
    }

    /// `refresh`, calling `probe` after the control file is read and before the read is applied.
    /// `probe` is passed whether the refresh holds the clock's lock at that moment.
    #[cfg(all(test, feature = "test-clock"))]
    fn refresh_with_read_probe(&self, probe: impl FnOnce(bool)) -> std::io::Result<bool> {
        self.refresh_with_reader(|path| {
            let bytes = std::fs::read(path);
            let serialized = self.inner.fixed.try_lock().is_err();
            probe(serialized);
            bytes
        })
    }

    /// The module may spawn this future while its daemon is running. In a release
    /// build it parks forever without polling the filesystem or emitting logs.
    pub async fn poll_changes(&self) -> std::io::Result<()> {
        #[cfg(feature = "test-clock")]
        {
            let mut poll = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                poll.tick().await;
                self.refresh()?;
            }
        }
        #[cfg(not(feature = "test-clock"))]
        std::future::pending().await
    }
}

#[cfg(test)]
mod tests {
    /// The poller's refresh and admission's refresh run concurrently. A poller read taken
    /// before the harness installs its fixed clock must not be applied after admission has
    /// installed it, or the clock would go back to real time. The refresh holds the clock's
    /// lock from its read to its application, so admission's refresh waits for the older one
    /// and then applies the newer file.
    #[cfg(feature = "test-clock")]
    #[test]
    fn a_held_refresh_read_cannot_put_the_clock_back_in_real_time() {
        let root = broca_test_support::scratch_root()
            .join(format!("clock-refresh-order-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("test-clock");
        let clock = super::Clock::for_state_root(&root).unwrap();
        let (read_tx, read_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let poll_clock = clock.clone();
        let poll = std::thread::spawn(move || {
            poll_clock.refresh_with_read_probe(|serialized| {
                // Hold the poller's read of the absent file, as when it runs before the
                // harness writes its clock.
                read_tx.send(serialized).unwrap();
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("release the held clock read");
            })
        });
        let serialized = read_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the poller read the absent clock file");
        assert!(
            serialized,
            "a refresh must hold the clock lock from its read until it applies it"
        );
        std::fs::write(&path, r#"{"now_ms":0}"#).unwrap();
        let admission_clock = clock.clone();
        let admission = std::thread::spawn(move || admission_clock.refresh());
        std::thread::sleep(std::time::Duration::from_millis(50));
        release_tx.send(()).unwrap();
        poll.join().unwrap().unwrap();
        admission.join().unwrap().unwrap();
        assert_eq!(clock.now_ms(), 0, "the fixed clock stays installed");
        let _ = std::fs::remove_dir_all(&root);
    }

    use super::*;

    #[test]
    fn clock_defaults_to_real_time() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let clock = Clock::default();
        let measured = u128::from(clock.now_ms());
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        assert!((before..=after).contains(&measured));
        assert_eq!(*clock.subscribe().borrow(), 0);
        assert!(!clock.refresh().unwrap());
    }

    #[cfg(not(feature = "test-clock"))]
    #[test]
    fn release_clock_ignores_control_file() {
        // An invalid path would fail construction if a release tried to read it.
        let clock = Clock::for_state_root("\0invalid-state-root").unwrap();
        assert!(!clock.refresh().unwrap());
        assert_eq!(*clock.subscribe().borrow(), 0);
    }

    #[cfg(feature = "test-clock")]
    #[tokio::test(start_paused = true)]
    async fn test_clock_refresh_poll_restart_and_rollback() {
        let root =
            broca_test_support::scratch_root().join(format!("core-clock-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("test-clock");
        std::fs::write(&path, r#"{"offset_ms":3601000}"#).unwrap();
        let clock = Clock::for_state_root(&root).unwrap();
        assert_eq!(*clock.subscribe().borrow(), 3601000);
        let restarted = Clock::for_state_root(&root).unwrap();
        assert_eq!(*restarted.subscribe().borrow(), 3601000);
        let mut changes = clock.subscribe();
        let polling_clock = clock.clone();
        let poll = tokio::spawn(async move { polling_clock.poll_changes().await });
        std::fs::write(&path, r#"{"offset_ms":-10000}"#).unwrap();
        changes.changed().await.unwrap();
        assert_eq!(*changes.borrow_and_update(), -10000);
        assert!(clock.now_ms() < Clock::default().now_ms());
        assert!(!clock.refresh().unwrap());
        std::fs::write(&path, "invalid").unwrap();
        assert_eq!(
            clock.refresh().unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(*changes.borrow(), -10000);
        poll.abort();
        std::fs::remove_dir_all(root).unwrap();
    }
}
