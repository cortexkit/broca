//! Deletion of what a session's committed changes stopped naming, run by the session's actor.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Deletes what the session's committed changes stopped naming, once it has gone unnamed for
    /// the grace period (see `broca_session_store::session::collect`). With nothing due, which
    /// is usual since a change's deletions come due a day after it, nothing is read or written.
    /// The actor is the session's only writer, so no append or seal runs meanwhile.
    pub(super) async fn collect_unreferenced(&mut self) {
        self.collect_due = false;
        let now_ms = (self.now_ms)();
        match self.session.collect(now_ms).await {
            Ok(0) => {}
            Ok(changes) => tracing::info!(
                target: "broca.store_actor",
                session = %self.key.session,
                changes,
                "deleted what the session's changes stopped naming",
            ),
            Err(error) => tracing::warn!(
                target: "broca.store_actor",
                session = %self.key.session,
                error = ?error,
                "what the session's changes stopped naming could not be deleted; the next open \
                 or the store's collection sweep retries",
            ),
        }
    }

    /// Runs deletions still due when the actor stops serving, unless the module is stopping or
    /// the session is about to be deleted whole.
    pub(super) async fn collect_on_exit(&mut self, exit: &Exit) {
        if self.collect_due
            && !matches!(exit, Exit::Stop | Exit::Expired(_) | Exit::Cut(_))
            && !self.created_and_untouched()
        {
            self.collect_unreferenced().await;
        }
    }
}
