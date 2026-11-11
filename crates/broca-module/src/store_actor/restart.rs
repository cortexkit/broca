//! Restart-paused runs wait for dependency registration, then continue under their original id.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    pub(super) fn restart_resumes_ready(&self) -> bool {
        self.registry
            .as_ref()
            .is_some_and(|registry| registry.restart_resume_ready())
    }

    pub(super) fn finish_restart_waiters(&mut self, result: Result<(), String>) {
        self.restart_admitting = false;
        for reply in std::mem::take(&mut self.restart_waiters) {
            let _ = reply.send(result.clone());
        }
    }

    pub(super) fn restart_stopping(&self) -> bool {
        self.stopping
            || self.seal.is_cancelled()
            || self
                .registry
                .as_ref()
                .is_some_and(|registry| registry.stopping_signal().is_cancelled())
    }

    pub(super) async fn resume_restart_pause(&mut self) -> Result<(), String> {
        if self.run.is_some() {
            return Ok(());
        }
        let Some(run) = self.session.operations().run.as_ref().filter(|run| {
            run.finished.is_none() && run.paused == Some(broca_protocol::PauseReason::Restart)
        }) else {
            return Ok(());
        };
        let cancelled = run.cancel.is_some();
        let run_id = RunId(run.run_id.clone());
        let prepared = self.recovery_start(run);
        // Caller intent still ends a pause; readiness gates only automatic continuation.
        if self.interrupt_waits_on_pause() {
            self.end_paused().await;
            return Ok(());
        }
        if !cancelled && !self.restart_resumes_ready() {
            return Ok(());
        }
        if self.restart_stopping() {
            return Err("shutdown interrupted restart admission".into());
        }
        let start = match prepared {
            Ok(start) => start,
            Err(error) => {
                if cancelled {
                    return Err(error);
                }
                self.end_restart_refused(&run_id, error, false).await?;
                Box::pin(self.drain()).await;
                return Ok(());
            }
        };
        if let Err(error) = self.track_recovery() {
            if cancelled {
                return Err(error);
            }
            self.end_restart_refused(&run_id, error, false).await?;
            Box::pin(self.drain()).await;
            return Ok(());
        }
        if cancelled {
            let cancel = CancellationToken::new();
            cancel.cancel();
            self.start_run_under(start, None, None, StoreRunMode::Resume, cancel)
                .await;
            return Ok(());
        }
        self.restart_admitting = true;
        self.start_run(start, None, None, StoreRunMode::Resume)
            .await;
        if self.run.is_none() {
            self.finish_restart_waiters(Err("restart resume did not start".into()));
            return Err("restart resume did not start".into());
        }
        Ok(())
    }

    pub(super) async fn end_restart_refused(
        &mut self,
        run_id: &RunId,
        message: String,
        recorded_resume: bool,
    ) -> Result<(), String> {
        let error = broca_protocol::ProviderError {
            class: broca_protocol::ErrorClass::Permanent,
            message,
            provider_code: Some("restart_resume_refused".into()),
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            status: None,
            retry_after_secs: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let now = (self.now_ms)();
        broca_session_loop::run::end_restart_resume_refused(
            &mut self.durable(),
            &run_id.0,
            &error,
            now,
            !recorded_resume,
        )
        .await
        .map_err(|error| format!("seal refused restart resume: {error:?}"))?;
        tracing::info!(target: "lifecycle", run_id = %run_id.0, session = %self.key.session,
            "restart resume refused; sealed interrupted");
        Ok(())
    }
}
