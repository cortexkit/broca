use super::*;
use broca_protocol::{ErrorClass, PauseReason, ProviderError, RunFinishReason};
use broca_session_store::fold::operations::Resolution;

/// A rebuildable scheduling hint. Only the current durable pause authorizes a wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TimedPause {
    run_id: RunId,
    pause_seq: u64,
    pub resume_not_before_ms: u64,
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    pub(super) async fn current_account_limit(&self) -> Result<Option<TimedPause>, String> {
        let Some(run) = self.session.operations().run.as_ref().filter(|run| {
            run.finished.is_none()
                && run.cancel.is_none()
                && run.resolution() == Resolution::Paused(PauseReason::AccountLimit)
        }) else {
            return Ok(None);
        };
        let pause: broca_wal::AccountLimitPause = serde_json::from_value(
            run.account_limit_pause
                .clone()
                .ok_or("account-limit pause has no recovery evidence")?,
        )
        .map_err(|error| format!("account-limit pause recovery evidence: {error}"))?;
        let selection: Option<broca_wal::AuthSelection> = match &run.refrozen_auth {
            Some(selection) => {
                Some(serde_json::from_value(selection.clone()).map_err(|error| {
                    format!("account-limit pause credential selection: {error}")
                })?)
            }
            None => self
                .latest_admission()?
                .and_then(|admission| admission.auth_selection),
        };
        let selection =
            selection.ok_or("account-limit pause has no frozen credential selection")?;
        if selection.credential_id != pause.credential_id {
            return Err(
                "account-limit pause does not match the frozen credential selection".into(),
            );
        }
        let start_seq = self
            .session
            .find_id(IdKind::RunId, &run.run_id)
            .await
            .map_err(|error| format!("{error:?}"))?
            .into_iter()
            .find_map(|(seq, record)| matches!(record, Record::RunStart(_)).then_some(seq))
            .ok_or("account-limit pause has no run start")?;
        let records = self
            .session
            .records_from(start_seq)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let (pause_seq, recorded) = records
            .iter()
            .rev()
            .find_map(|(seq, record)| match record {
                Record::Pause(recorded) if recorded.run_id == run.run_id => Some((*seq, recorded)),
                _ => None,
            })
            .ok_or("account-limit pause has no recorded sequence")?;
        let error: Option<ProviderError> = recorded
            .error
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| format!("account-limit pause source error: {error}"))?;
        if recorded.reason != PauseReason::AccountLimit
            || recorded.account_limit != run.account_limit_pause
        {
            return Err("account-limit pause does not match its folded evidence".into());
        }
        if error
            .as_ref()
            .is_none_or(|error| error.class != ErrorClass::Transient)
        {
            return Err("account-limit pause does not carry a transient source error".into());
        }
        let matched_attempt = records.iter().any(|(seq, record)| {
            if *seq != pause.failed_attempt_seq || *seq >= pause_seq {
                return false;
            }
            matches!(record, Record::Attempt(attempt)
                if attempt.run_id == run.run_id
                    && attempt.step_id == pause.failed_step.0
                    && attempt.attempt == pause.attempt
                    && attempt.request_body_len == pause.request_body_len
                    && broca_session_store::codec::hex(&attempt.request_body_sha256.0) == pause.request_body_sha256
                    && pause.attempt_id.as_ref().is_none_or(|id| Some(id.as_str()) == attempt.attempt_id.as_deref())
                    && attempt.billing.as_ref().and_then(|billing| billing.get("account_limit_disposition"))
                        == serde_json::to_value(pause.disposition).ok().as_ref())
        });
        if !matched_attempt {
            return Err(
                "account-limit pause does not reference its source-classified failed attempt"
                    .into(),
            );
        }
        Ok(Some(TimedPause {
            run_id: RunId(run.run_id.clone()),
            pause_seq,
            resume_not_before_ms: pause.resume_not_before_ms,
        }))
    }

    pub(super) async fn arm_account_limit(&mut self) {
        self.account_limit_timer = None;
        if self.run.is_some() {
            return;
        }
        match self.current_account_limit().await {
            Ok(pause) => self.account_limit_timer = pause,
            Err(error) => {
                tracing::warn!(target: "broca.store_actor", %error, "account-limit timer could not be rebuilt")
            }
        }
    }

    pub(super) async fn wake_account_limit(&mut self, expected: TimedPause) -> Result<(), String> {
        if self.run.is_some()
            || self.stopping
            || self
                .registry
                .as_ref()
                .is_some_and(|registry| registry.is_stopping())
        {
            return Ok(());
        }
        self.account_limit_timer = None;
        let Some(pause) = self.current_account_limit().await? else {
            return Ok(());
        };
        if pause != expected
            || (self.now_ms)() < pause.resume_not_before_ms
            || self
                .registry
                .as_ref()
                .is_some_and(|registry| !registry.restart_resume_ready())
        {
            self.account_limit_timer = Some(pause);
            return Ok(());
        }
        let run = self
            .session
            .operations()
            .run
            .as_ref()
            .expect("the pause owns the latest run");
        let start = self.recovery_start(run).and_then(|mut start| {
            // The recovery request provides routing information, but the actor also reads
            // keep-warm before handing the run to the executor. Restore the recorded policy here.
            start.request.keep_warm = run
                .keep_warm
                .clone()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| format!("account-limit pause keep-warm policy: {error}"))?;
            Ok(start)
        });
        match start {
            Ok(start) => {
                self.start_run(start, None, None, StoreRunMode::Resume)
                    .await;
                if let Some(active) = self.run.as_mut() {
                    active.account_limit_resume = true;
                }
            }
            Err(error) => {
                self.refuse_account_limit_resume(&pause.run_id, error)
                    .await?;
                self.drain().await;
            }
        }
        Ok(())
    }

    pub(super) async fn refuse_account_limit_resume(
        &mut self,
        run_id: &RunId,
        message: String,
    ) -> Result<(), String> {
        if !self
            .session
            .operations()
            .run
            .as_ref()
            .is_some_and(|run| run.run_id == run_id.0 && run.finished.is_none())
        {
            return Ok(());
        }
        let error = ProviderError {
            class: ErrorClass::Permanent,
            message,
            provider_code: Some("account_limit_resume_refused".into()),
            status: None,
            retry_after_secs: None,
            provider_subcode: None,
            refusal: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            provider_details: None,
        };
        let now = (self.now_ms)();
        broca_session_loop::run::end_run_with_error_and_interrupt_reason(
            &mut self.durable(),
            &run_id.0,
            RunFinishReason::Interrupted,
            &error,
            now,
            Some("runner_crash".into()),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
        self.bill_owed().await;
        Ok(())
    }
}
