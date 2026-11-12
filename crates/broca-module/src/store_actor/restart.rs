//! Restart-paused runs wait for dependency registration, then continue under their original id.
//! A run whose pause is older than [`RESTART_RESUME_WINDOW_MS`] is not continued: it ends
//! Interrupted.

use super::*;

/// How long after a run paused for a restart broca still continues it by itself.
///
/// A restart takes seconds to minutes. A pause older than this was left by a long outage, or
/// by a build that could not resume it, and whoever sent the run has stopped waiting for it:
/// continuing would spend model calls on an answer nobody reads. Such a run ends Interrupted,
/// which is what happened to it, and its caller decides whether to send again.
const RESTART_RESUME_WINDOW_MS: u64 = 60 * 60 * 1000;

/// Whether a restart pause written at `paused_at_ms` is too old at `now_ms` to continue. A
/// pause that records no time has an unknown age, and an unknown age is not a recent one: the
/// run loop always records the time, so only a record it didn't write lacks it.
fn restart_pause_expired(paused_at_ms: Option<u64>, now_ms: u64) -> bool {
    paused_at_ms.is_none_or(|at| now_ms.saturating_sub(at) > RESTART_RESUME_WINDOW_MS)
}

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
        // An old pause ends here whether or not the run's dependencies have registered: it
        // needs none of them. A cancelled run skips this and ends Cancelled below, as its
        // caller asked.
        if !cancelled {
            let paused_at_ms = self.restart_paused_at_ms(&run_id.0).await?;
            let now_ms = (self.now_ms)();
            if restart_pause_expired(paused_at_ms, now_ms) {
                let message = match paused_at_ms {
                    Some(at) => format!(
                        "the run paused for a restart {} s ago; a pause is continued for {} s",
                        now_ms.saturating_sub(at) / 1000,
                        RESTART_RESUME_WINDOW_MS / 1000
                    ),
                    None => "the run's restart pause records no time, so its age is unknown".into(),
                };
                self.end_restart_interrupted(&run_id, "restart_resume_expired", message, false)
                    .await?;
                Box::pin(self.drain()).await;
                return Ok(());
            }
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

    /// When the session's unfinished run `run_id` paused for a restart, read from its pause
    /// record. `None` when that record carries no time.
    async fn restart_paused_at_ms(&self, run_id: &str) -> Result<Option<u64>, String> {
        let start_seq = self
            .session
            .find_id(IdKind::RunId, run_id)
            .await
            .map_err(|error| format!("{error:?}"))?
            .into_iter()
            .find_map(|(seq, record)| matches!(record, Record::RunStart(_)).then_some(seq))
            .ok_or("restart pause has no run start")?;
        let records = self
            .session
            .records_from(start_seq)
            .await
            .map_err(|error| format!("{error:?}"))?;
        records
            .iter()
            .rev()
            .find_map(|(_, record)| match record {
                Record::Pause(pause)
                    if pause.run_id == run_id
                        && pause.reason == broca_protocol::PauseReason::Restart =>
                {
                    Some(pause.ts_ms)
                }
                _ => None,
            })
            .ok_or_else(|| "restart pause has no pause record".to_owned())
    }

    /// Ends a run whose restart resume was refused: Interrupted, with `restart_resume_refused`.
    pub(super) async fn end_restart_refused(
        &mut self,
        run_id: &RunId,
        message: String,
        recorded_resume: bool,
    ) -> Result<(), String> {
        self.end_restart_interrupted(run_id, "restart_resume_refused", message, recorded_resume)
            .await
    }

    /// Ends a restart-paused run Interrupted without continuing it, under `code`: the resume
    /// was refused, or the pause is too old to continue.
    async fn end_restart_interrupted(
        &mut self,
        run_id: &RunId,
        code: &str,
        message: String,
        recorded_resume: bool,
    ) -> Result<(), String> {
        let error = broca_protocol::ProviderError {
            class: broca_protocol::ErrorClass::Permanent,
            message,
            provider_code: Some(code.into()),
            provider_details: None,
            cause: None,
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
        tracing::info!(target: "lifecycle", run_id = %run_id.0, session = %self.key.session, code,
            "restart-paused run not continued; sealed interrupted");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{restart_pause_expired, RESTART_RESUME_WINDOW_MS};

    #[test]
    fn a_restart_pause_expires_after_the_window_and_when_its_time_is_unknown() {
        let paused_at = 1_000_000;
        assert!(!restart_pause_expired(Some(paused_at), paused_at));
        assert!(!restart_pause_expired(
            Some(paused_at),
            paused_at + RESTART_RESUME_WINDOW_MS
        ));
        assert!(restart_pause_expired(
            Some(paused_at),
            paused_at + RESTART_RESUME_WINDOW_MS + 1
        ));
        // A clock that reads earlier than the pause gives no age to exceed the window.
        assert!(!restart_pause_expired(Some(paused_at), paused_at - 1));
        assert!(restart_pause_expired(None, paused_at));
    }
}
