//! Provider-held continuations rejoin their original batch; they never dispatch it again.

use super::*;
use broca_session_store::fold::operations::Resolution;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// An open held batch stays parked until a late result wakes it. A crash may have lost
    /// the pause after the held record, so make that boundary durable before serving commands.
    pub(super) async fn park_held(&mut self) -> bool {
        let Some(run) = self.session.operations().run.as_ref() else {
            return false;
        };
        if !matches!(run.resolution(), Resolution::ProviderHeld(waiting) if !waiting.is_empty()) {
            return false;
        }
        if run.paused != Some(broca_protocol::PauseReason::ToolHeld) {
            let record = Record::Pause(broca_session_store::record::Pause {
                run_id: run.run_id.clone(),
                reason: broca_protocol::PauseReason::ToolHeld,
                ts_ms: broca_core::clock::epoch_millis(),
                error: None,
                account_limit: None,
            });
            if let Err(error) = self.append_own(record).await {
                tracing::error!(target: "broca.store_actor", %error, "could not park a provider-held run on recovery");
            }
        }
        true
    }

    pub(super) async fn resume_held(&mut self) -> Result<(), String> {
        if self.is_accepting_drain() && self.run.is_none() {
            return Ok(());
        }
        if self.run.is_some() {
            // The live batch usually takes the deposited result directly. If it has already
            // decided to park, its terminal handler owes the continuation instead.
            self.held_resume_owed = true;
            return Ok(());
        }
        // A module shutting down doesn't take up a held run: it stays parked for the next
        // process, which a resume under the stop would only find already stopping.
        if self.restart_stopping() {
            return Err("session actor is closing".into());
        }
        let Some(run) = self.session.operations().run.as_ref().filter(|run| {
            run.finished.is_none() && matches!(run.resolution(), Resolution::ProviderHeld(_))
        }) else {
            return Ok(());
        };
        let start = self.recovery_start(run)?;
        self.start_run(start, None, None, StoreRunMode::Resume)
            .await;
        Ok(())
    }
}
