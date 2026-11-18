//! A moved session's first run here (see `store_open::moved_here`).

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    fn session_address(&self) -> broca_session_store::backend::SessionAddr {
        broca_session_store::backend::SessionAddr(broca_core::session_addr(&self.key))
    }

    /// Whether the session moved here and no run has started here since, so the next run names
    /// the credential it last ran on.
    pub(super) fn moved_here(&self) -> bool {
        self.recovery_index.as_deref().is_some_and(|root| {
            crate::store_open::moved_here::is_marked(root, &self.session_address())
        })
    }

    /// Clears the mark once `run_id`'s start is durable here: routing admitted the run and now
    /// keeps the conversation's account itself. A run refused before its start leaves the mark.
    pub(super) fn settle_moved_here(&self, run_id: &RunId) {
        let Some(root) = self.recovery_index.as_deref() else {
            return;
        };
        let started = self
            .session
            .operations()
            .run
            .as_ref()
            .is_some_and(|run| run.run_id == run_id.0 && run.start.is_some());
        if !started || !self.moved_here() {
            return;
        }
        if let Err(error) = crate::store_open::moved_here::clear(root, &self.session_address()) {
            tracing::warn!(target: "lifecycle", %error, "could not clear a moved session's mark");
        }
    }
}
