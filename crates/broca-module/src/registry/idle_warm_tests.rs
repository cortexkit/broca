use super::*;
use crate::registry::{ActorHandle, LineageOpener, OpenError};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

struct Opener {
    opens: Arc<AtomicU64>,
    warms: tokio::sync::mpsc::UnboundedSender<bool>,
    store: bool,
}

#[async_trait::async_trait]
impl LineageOpener for Opener {
    /// A session store whose runtime never opens: this opener keeps no sessions.
    fn store_slot(&self) -> &std::sync::Arc<crate::store_open::StoreSlot> {
        static SLOT: std::sync::OnceLock<std::sync::Arc<crate::store_open::StoreSlot>> =
            std::sync::OnceLock::new();
        SLOT.get_or_init(|| {
            std::sync::Arc::new(crate::store_open::StoreSlot::awaiting_key(
                std::path::Path::new("/nonexistent-broca-test-store"),
                1,
            ))
        })
    }
    fn serves_from_store(&self, _: &LineageKey) -> Result<bool, OpenError> {
        // Stands in for a head the session store keeps, without writing one.
        Ok(self.store)
    }

    async fn open(
        &self,
        _: &LineageKey,
        gen: u64,
        _: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        let (tx, mut commands) = tokio::sync::mpsc::channel(8);
        let warms = self.warms.clone();
        tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                if let crate::actor::LineageCmd::Warm {
                    idle_auto, reply, ..
                } = command
                {
                    warms.send(idle_auto).unwrap();
                    let _ = reply.send(crate::actor::WarmOutcome::Error("test refusal".into()));
                }
            }
        });
        Ok(ActorHandle {
            tx,
            gen,
            run_in_flight: Arc::new(AtomicU64::new(0)),
            first_episode: Arc::default(),
        })
    }
}

#[tokio::test(start_paused = true)]
async fn idle_timer_opens_closed_store_actor_before_hour_expiry_and_refusal_stays_stopped() {
    let opens = Arc::new(AtomicU64::new(0));
    let (warms, mut calls) = tokio::sync::mpsc::unbounded_channel();
    let registry = LineageRegistry::new(Arc::new(Opener {
        opens: opens.clone(),
        warms,
        store: true,
    }));
    let key = LineageKey::new("/project", "test", "head");
    let handle = registry.handle();
    handle.arm_idle_warm(&key, 3_570_000, 0);
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_millis(3_569_999)).await;
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    assert!(calls.recv().await.unwrap());
    broca_test_support::wait_until("idle warm stops after refusal", || async {
        if !handle.has_idle_warm(&key) {
            Ok(())
        } else {
            Err("still armed".into())
        }
    })
    .await;
    handle.arm_idle_warm(&key, 4_000_000, 3_570_000);
    assert!(
        !handle.has_idle_warm(&key),
        "an actor reopen must not retry a refusal"
    );
    handle.stop_idle_warm(&key, "real_run");
    handle.arm_idle_warm(&key, 4_000_000, 3_570_000);
    assert!(
        handle.has_idle_warm(&key),
        "a real run resets the stopped state"
    );
    handle.stop_idle_warm(&key, "shutdown");
}

#[tokio::test(start_paused = true)]
async fn idle_timer_cancellation_and_wal_storage_never_dispatch_warm() {
    let opens = Arc::new(AtomicU64::new(0));
    let (warms, mut calls) = tokio::sync::mpsc::unbounded_channel();
    let registry = LineageRegistry::new(Arc::new(Opener {
        opens: opens.clone(),
        warms,
        store: false,
    }));
    let key = LineageKey::new("/project", "test", "head");
    let handle = registry.handle();
    handle.arm_idle_warm(&key, 100, 0);
    handle.stop_idle_warm(&key, "real_run");
    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    handle.arm_idle_warm(&key, 200, 100);
    broca_test_support::wait_until("a session with no stored log refuses idle warm", || async {
        if !handle.has_idle_warm(&key) {
            Ok(())
        } else {
            Err("still armed".into())
        }
    })
    .await;
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    assert!(calls.try_recv().is_err());
}

/// A test clock moves without real time passing. An armed warm fires once the daemon's clock
/// reaches its due time, not after the real wait it was armed with (an hour here).
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn an_idle_warm_fires_when_the_daemon_clock_reaches_it() {
    let dir =
        broca_test_support::scratch_root().join(format!("idle-warm-clock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let control = dir.join("test-clock");
    std::fs::write(&control, r#"{"now_ms":1000}"#).unwrap();
    let leases = Arc::new(broca_core::FileLeaseStore::new(dir.join("leases")));
    let service = Arc::new(crate::retention::RetentionService::new(&dir, leases).unwrap());
    let (warms, mut calls) = tokio::sync::mpsc::unbounded_channel();
    let registry = LineageRegistry::new(Arc::new(Opener {
        opens: Arc::new(AtomicU64::new(0)),
        warms,
        store: true,
    }));
    registry.set_retention_service(service.clone());
    let key = LineageKey::new("/project", "test", "head");
    let due = 1000 + 3_600_000;
    registry.handle().arm_idle_warm(&key, due, 1000);
    std::fs::write(&control, format!(r#"{{"now_ms":{due}}}"#)).unwrap();
    assert!(service.clock.refresh().unwrap());
    let fired = tokio::time::timeout(std::time::Duration::from_secs(30), calls.recv()).await;
    assert_eq!(
        fired.ok().flatten(),
        Some(true),
        "the warm fires at its due time on the daemon's clock"
    );
    registry.handle().stop_idle_warm(&key, "shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
