//! Retention timers are independent of the idle-close timer and of attached subscribers.

use super::*;

/// When the session comes due for deletion, as the actor last decided it. The older retention
/// seconds are folded from the session's records one record at a time: each decision reads only
/// the records appended since the last one.
#[derive(Default)]
pub(super) struct Timer {
    /// The seq of the first record not folded into `fold` yet: every record before it is.
    at: u64,
    fold: crate::retention::PolicyFold,
    /// How many records the fold has read, for tests that pin how much a decision reads.
    #[cfg(test)]
    records_read: usize,
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

    /// The deadline the older retention seconds give the session (`store_policy`); `None` when
    /// it was sent none. When its records can't be read, a second from now, so retention is
    /// decided again then.
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
        if self.catch_up(session).await.is_err() {
            return Some(clock.now_ms().saturating_add(1000));
        }
        self.fold.decided().2
    }

    /// The retention seconds a new send of the session is checked against and keeps when it asks
    /// for none: the shortest any of its run starts or sends set, queued sends included, since
    /// each was admitted against the ones before it. `None` when its first run set none.
    pub(super) async fn seconds<B: SessionBackend>(
        &mut self,
        session: &Session<B>,
    ) -> Result<Option<u64>, SessionError> {
        if session
            .operations
            .owner
            .first_start
            .as_ref()
            .and_then(|start| start.retention_secs)
            .is_none()
        {
            return Ok(None);
        }
        self.catch_up(session).await?;
        Ok(self.fold.seconds())
    }

    /// Folds the records appended since the last fold. Reading the whole session instead would
    /// cost a long session its log's length after every write.
    async fn catch_up<B: SessionBackend>(
        &mut self,
        session: &Session<B>,
    ) -> Result<(), SessionError> {
        let next = session.next_seq();
        if next < self.at {
            // Never expected: a session's seqs only grow. Fold it again from its start.
            *self = Self::default();
        }
        if next == self.at {
            return Ok(());
        }
        let records = session.records_from(self.at).await?;
        #[cfg(test)]
        {
            self.records_read += records.len();
        }
        for (_, record) in &records {
            if let Some(event) = crate::retention::store::policy_event(record) {
                self.fold.apply(event);
            }
        }
        self.at = next;
        Ok(())
    }

    /// The session's deadline as retention's pass would read it from its records
    /// (`retention::store::session_deadline`), when this timer already knows it at the session's
    /// current state without reading anything: `None` when it doesn't, as for older retention
    /// seconds not read since the last write, or a frozen policy that no longer decodes.
    pub(super) fn known<B: SessionBackend>(&self, session: &Session<B>) -> Option<Option<u64>> {
        let operations = &session.operations;
        let legacy = || {
            let first = operations.owner.first_start.as_ref()?;
            if first.retention_secs.is_none() {
                return Some(None);
            }
            (self.at == session.next_seq()).then(|| self.fold.decided().2)
        };
        if operations.retention_policy.is_none() && operations.released_at_ms.is_none() {
            return legacy();
        }
        let starter = match operations.retention_policy.as_ref() {
            Some(value) => Some(
                cortexkit_role_llm_runner::session_retention::RetentionPolicy::from_value(value)
                    .ok()?,
            ),
            None => None,
        };
        let legacy = match starter {
            Some(_) => None,
            None => legacy()?,
        };
        let busy = operations
            .run
            .as_ref()
            .is_some_and(|run| run.finished.is_none())
            || !operations.queue.is_empty();
        Some(crate::retention::policy::deadline(
            starter.as_ref(),
            operations.released_at_ms,
            legacy,
            busy,
            session.open_log_activity_ms(),
        ))
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

#[cfg(test)]
mod tests;
