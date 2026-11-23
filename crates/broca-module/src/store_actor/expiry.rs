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
        if active {
            return None;
        }
        let operations = &session.operations;
        // A session whose first policy-carrying send froze a policy, or whose owner released
        // it, is decided from its folded state (`operations`) alone: reading every record on
        // each write would cost a long session its log's length on every append.
        if operations.retention_policy.is_some() || operations.released_at_ms.is_some() {
            let starter = match operations.retention_policy.as_ref() {
                Some(value) => Some(
                    cortexkit_role_llm_runner::session_retention::RetentionPolicy::from_value(
                        value,
                    )
                    // A damaged policy deletes nothing; the sweep reports it.
                    .ok()?,
                ),
                None => None,
            };
            let busy = operations
                .run
                .as_ref()
                .is_some_and(|run| run.finished.is_none())
                || !operations.queue.is_empty();
            // A starter's policy replaces the older retention seconds; a release ends either.
            let legacy = match starter {
                Some(_) => None,
                None => self.legacy(session, clock).await,
            };
            return crate::retention::policy::deadline(
                starter.as_ref(),
                operations.released_at_ms,
                legacy,
                busy,
                session.open_log_activity_ms(),
            );
        }
        self.legacy(session, clock).await
    }

    /// The deadline the older retention seconds give the session (`store_policy`), read from its
    /// records again only once it has changed; `None` when it was sent none. When its records
    /// can't be read, a second from now, so retention is decided again then.
    async fn legacy<B: SessionBackend>(
        &mut self,
        session: &Session<B>,
        clock: &broca_core::clock::Clock,
    ) -> Option<u64> {
        session
            .operations
            .owner
            .first_start
            .as_ref()
            .and_then(|start| start.retention_secs)?;
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

/// Deletes the session `key`, whose actor closed because its retention deadline came, and says
/// whether it was deleted; one that couldn't be is left to the sweep.
pub(super) async fn delete(registry: &crate::registry::RegistryHandle, key: &BindIdentity) -> bool {
    let (Some(service), Some(runtime)) = (registry.retention_service(), registry.store_runtime())
    else {
        return false;
    };
    match service
        .delete_store_under_lease(&runtime, key, service.clock.now_ms())
        .await
    {
        Ok(deleted) => deleted,
        Err(error) => {
            tracing::warn!(target: "retention", %error, "expired content deletion deferred to sweep");
            false
        }
    }
}
