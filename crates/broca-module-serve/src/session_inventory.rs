//! Single-flight, signalled discovery for binds that need to prove absence.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_module::registry::SessionInventoryReadiness;
use broca_store::session_address::{InventoryUnavailable, SessionAddressError};
use tokio::sync::watch;

// Revalidating a production-sized, already-covered index measured 2.77 seconds.
// Use twenty times that duration, rounded up to a minute, as a bounded allowance
// for colder disks and foreground writers, not a worst-case census estimate.
const INVENTORY_READINESS_WAIT: Duration = Duration::from_secs(60);

type Completion = Option<Result<(), InventoryUnavailable>>;

struct Inner {
    root: PathBuf,
    index: Arc<broca_store::SqliteStore>,
    running: Mutex<Option<watch::Receiver<Completion>>>,
    #[cfg(all(test, feature = "test-clock"))]
    waiting: watch::Sender<usize>,
    #[cfg(all(test, feature = "test-clock"))]
    attempts: std::sync::atomic::AtomicUsize,
}

#[derive(Clone)]
pub(crate) struct SessionInventory {
    inner: Arc<Inner>,
}

impl SessionInventory {
    pub(crate) fn new(root: PathBuf, index: Arc<broca_store::SqliteStore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                root,
                index,
                running: Mutex::new(None),
                #[cfg(all(test, feature = "test-clock"))]
                waiting: watch::channel(0).0,
                #[cfg(all(test, feature = "test-clock"))]
                attempts: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    async fn begin(&self) -> Result<Option<watch::Receiver<Completion>>, InventoryUnavailable> {
        let inner = self.inner.clone();
        broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::Recovery, move || {
            let mut running = inner
                .running
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(receiver) = running.as_ref() {
                if receiver.borrow().is_none() {
                    return Ok(Some(receiver.clone()));
                }
            }
            if inner
                .index
                .session_refs(std::iter::empty())
                .map_err(|error| InventoryUnavailable::MigrationFailed(error.to_string()))?
                .complete
            {
                return Ok(None);
            }
            let (sender, receiver) = watch::channel(None);
            *running = Some(receiver.clone());
            #[cfg(all(test, feature = "test-clock"))]
            inner
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            crate::session_address_health::record_repair("started");
            let worker = inner.clone();
            tokio::spawn(async move {
                let result = broca_diagnostics::spawn_blocking(
                    broca_diagnostics::JobKind::Recovery,
                    move || repair(&worker),
                )
                .await
                .unwrap_or_else(|error| {
                    Err(InventoryUnavailable::MigrationFailed(error.to_string()))
                });
                crate::session_address_health::record_repair(if result.is_ok() {
                    "completed"
                } else {
                    "failed"
                });
                if let Err(error) = &result {
                    tracing::warn!(%error, "session-ref inventory repair did not complete");
                }
                sender.send_replace(Some(result));
            });
            Ok(Some(receiver))
        })
        .await
        .map_err(|error| InventoryUnavailable::MigrationFailed(error.to_string()))?
    }
}

fn repair(inner: &Inner) -> Result<(), InventoryUnavailable> {
    broca_store::session_refs::migrate(
        &inner.root.join("run-index.db"),
        &broca_core::ReadOnlyWalReader::new(&inner.root),
    )
    .map_err(|error| InventoryUnavailable::MigrationFailed(error.to_string()))?;
    if inner
        .index
        .session_refs(std::iter::empty())
        .map_err(|error| InventoryUnavailable::MigrationFailed(error.to_string()))?
        .complete
    {
        return Ok(());
    }
    if broca_core::session_projection_incomplete(&inner.root)
        .map_err(|error| InventoryUnavailable::MigrationFailed(error.to_string()))?
    {
        return Err(InventoryUnavailable::GapMarker);
    }
    Err(InventoryUnavailable::MigrationFailed(
        "discovery left the inventory incomplete".into(),
    ))
}

#[async_trait]
impl SessionInventoryReadiness for SessionInventory {
    fn start(&self) {
        let inventory = self.clone();
        tokio::spawn(async move {
            if let Err(error) = inventory.begin().await {
                tracing::warn!(%error, "cannot start session-ref inventory repair");
            }
        });
    }

    async fn wait(&self) -> Result<(), SessionAddressError> {
        let wait = async {
            let Some(mut receiver) = self.begin().await? else {
                return Ok(());
            };
            #[cfg(all(test, feature = "test-clock"))]
            self.inner.waiting.send_modify(|waiting| *waiting += 1);
            loop {
                if let Some(result) = receiver.borrow().clone() {
                    return result;
                }
                receiver.changed().await.map_err(|_| {
                    InventoryUnavailable::MigrationFailed("repair readiness signal closed".into())
                })?;
            }
        };
        tokio::time::timeout(INVENTORY_READINESS_WAIT, wait)
            .await
            .unwrap_or(Err(InventoryUnavailable::WaitExpired))
            .map_err(SessionAddressError::Unavailable)
    }
}

#[cfg(all(test, feature = "test-clock"))]
mod tests;
