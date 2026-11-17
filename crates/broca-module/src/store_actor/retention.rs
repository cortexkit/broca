//! A retained send's retention. Whether a session is retained is fixed by its first send; a later
//! send may keep its retention or shorten it, never lengthen it.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// The retention a send's run starts with: the one it asks for, once checked against the
    /// session's current one, else the current one. The current one is the latest run's, which a
    /// send that shortened it set, else the first run's.
    pub(super) async fn send_retention(
        &self,
        requested: Option<u64>,
    ) -> Result<Option<u64>, SendOutcome> {
        let operations = self.session.operations();
        let first = operations.owner.first_start.as_ref();
        let current = operations
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
            .and_then(|start| start.retention_secs)
            .or_else(|| first.and_then(|start| start.retention_secs));
        let exists = first.is_some();
        crate::retention::validate_retention(requested, current, exists)
            .map_err(SendOutcome::RetentionInvalid)?;
        Ok(requested.or(current))
    }

    pub(super) fn retention_timestamp(&self) -> Option<u64> {
        self.session
            .operations()
            .owner
            .first_start
            .as_ref()?
            .retention_secs?;
        Some(
            self.retention_clock
                .as_ref()
                .map_or_else(crate::actor::epoch_ms, |clock| clock.now_ms()),
        )
    }
}
