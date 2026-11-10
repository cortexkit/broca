//! Retained sends use the same immutable first-send policy as today's actor.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    pub(super) async fn send_retention(
        &self,
        requested: Option<u64>,
    ) -> Result<Option<u64>, SendOutcome> {
        let first = self.session.operations().owner.first_start.as_ref();
        let current = first.and_then(|start| start.retention_secs);
        let exists = first.is_some();
        crate::retention::validate_retention(requested, current, exists)
            .map_err(SendOutcome::RetentionInvalid)?;
        Ok(current.or(requested))
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
