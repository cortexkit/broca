//! Retention timers are independent of the idle-close timer and of attached subscribers.

use super::*;

#[derive(Default)]
pub(super) struct Timer {
    at: Option<u64>,
    deadline: Option<u64>,
}

impl Timer {
    pub(super) async fn deadline<B: SessionBackend>(
        &mut self,
        session: &Session<B>,
        active: bool,
        clock: Option<&broca_core::clock::Clock>,
    ) -> Option<u64> {
        let clock = clock?;
        if active
            || session
                .operations
                .owner
                .first_start
                .as_ref()
                .and_then(|start| start.retention_secs)
                .is_none()
        {
            return None;
        }
        if self.at != Some(session.next_seq()) {
            match session.records_from(0).await {
                Ok(records) => {
                    self.deadline = crate::retention::store::store_policy(&records)
                        .and_then(|(_, _, deadline)| deadline);
                    self.at = Some(session.next_seq());
                }
                Err(_) => return Some(clock.now_ms().saturating_add(1000)),
            }
        }
        self.deadline
    }
}

pub(super) async fn wait(deadline: Option<u64>, clock: &broca_core::clock::Clock) {
    match deadline {
        Some(deadline) => {
            tokio::time::sleep(std::time::Duration::from_millis(
                deadline.saturating_sub(clock.now_ms()),
            ))
            .await
        }
        None => std::future::pending().await,
    }
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    pub(crate) fn with_retention_clock(mut self, clock: broca_core::clock::Clock) -> Self {
        let now = clock.clone();
        self.now_ms = Arc::new(move || now.now_ms());
        self.retention_clock = Some(clock);
        self
    }
}

pub(super) async fn delete(registry: &crate::registry::RegistryHandle, key: &BindIdentity) {
    if let (Some(service), Some(runtime)) = (registry.retention_service(), registry.store_runtime())
    {
        if let Err(error) = service
            .delete_store_under_lease(&runtime, key, service.clock.now_ms())
            .await
        {
            tracing::warn!(target: "retention", %error, "expired content deletion deferred to sweep");
        }
    }
}
