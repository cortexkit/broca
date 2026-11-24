use super::*;
use broca_protocol::{ErrorClass, PauseReason, ProviderError, RunFinishReason};
use broca_session_store::fold::operations::Resolution;

/// A rebuildable scheduling hint. Only the current durable pause authorizes a wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TimedPause {
    run_id: RunId,
    pause_seq: u64,
    pub resume_not_before_ms: u64,
    sign_in: bool,
}

/// Routing's completed sign-in continuation answer and retry schedule for one durable pause.
#[derive(Debug, Clone)]
pub(super) struct SignInCheck {
    pause: TimedPause,
    outcome: broca_session_loop::account_move::SignInOutcome,
    failed_checks: u32,
    retry_not_before_ms: u64,
}

impl SignInCheck {
    fn answer_floor(&self) -> u64 {
        match self.outcome {
            broca_session_loop::account_move::SignInOutcome::Hold { answered_at_ms, .. } => {
                answered_at_ms.saturating_add(30_000)
            }
            _ => 0,
        }
    }
    fn wake_ms(&self) -> Option<u64> {
        match self.outcome {
            broca_session_loop::account_move::SignInOutcome::Hold {
                until_ms: Some(until),
                ..
            } => Some(until.max(self.answer_floor()).max(self.retry_not_before_ms)),
            _ => None,
        }
    }
    fn may_ask(&self, now: u64) -> bool {
        match self.outcome {
            broca_session_loop::account_move::SignInOutcome::Hold { until_ms, .. } => {
                now >= self.answer_floor().max(self.retry_not_before_ms)
                    && until_ms.is_none_or(|until| now >= until)
            }
            _ => false,
        }
    }
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
        let pause: broca_records::record::AccountLimitPause = serde_json::from_value(
            run.account_limit_pause
                .clone()
                .ok_or("account-limit pause has no recovery evidence")?,
        )
        .map_err(|error| format!("account-limit pause recovery evidence: {error}"))?;
        let selection: Option<broca_records::record::AuthSelection> = match &run.refrozen_auth {
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
            .as_ref()
            .map(|error| serde_json::from_value(error.parse()))
            .transpose()
            .map_err(|error| format!("account-limit pause source error: {error}"))?;
        if recorded.reason != PauseReason::AccountLimit
            || recorded
                .account_limit
                .as_ref()
                .map(broca_protocol::json::Json::parse)
                != run.account_limit_pause
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
                    && attempt.billing.as_ref().map(broca_protocol::json::Json::parse).as_ref().and_then(|billing| billing.get("account_limit_disposition"))
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
            sign_in: false,
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
        if self.account_limit_timer.is_none() {
            let Some(check) = &self.sign_in_check else {
                return;
            };
            let Some(wake) = check.wake_ms() else {
                return;
            };
            let expected = check.pause.clone();
            if let Ok(Some(mut current)) = self.current_sign_in().await {
                if current.run_id == expected.run_id && current.pause_seq == expected.pause_seq {
                    current.resume_not_before_ms = wake;
                    self.account_limit_timer = Some(current);
                }
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
        if expected.sign_in {
            let Some(current) = self.current_sign_in().await? else {
                return Ok(());
            };
            if current.run_id != expected.run_id || current.pause_seq != expected.pause_seq {
                return Ok(());
            }
            if (self.now_ms)() < expected.resume_not_before_ms {
                self.account_limit_timer = Some(expected);
                return Ok(());
            }
            let start = self.recovery_start(
                self.session
                    .operations()
                    .run
                    .as_ref()
                    .expect("current sign-in pause"),
            )?;
            let mode = self.sign_in_recheck_mode().await;
            self.start_run(start, None, None, mode).await;
            if let Some(active) = self.run.as_mut() {
                active.preserve_auth_pause = true;
            }
            return Ok(());
        }
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
        // A resumed run's warming is read from its start, as for every resume.
        let start = self.recovery_start(run);
        match start {
            Ok(start) => {
                self.start_run(start, None, None, StoreRunMode::Resume)
                    .await;
                if let Some(active) = self.run.as_mut() {
                    active.account_limit_resume = true;
                }
            }
            Err(error) => {
                self.refuse_account_limit_resume(&pause.run_id, ErrorClass::Permanent, error)
                    .await?;
                self.drain().await;
            }
        }
        Ok(())
    }

    pub(super) async fn current_sign_in(&self) -> Result<Option<TimedPause>, String> {
        let Some(run) = self.session.operations().run.as_ref().filter(|run| {
            run.finished.is_none()
                && run.cancel.is_none()
                && run.resolution() == Resolution::Paused(PauseReason::AuthRequired)
        }) else {
            return Ok(None);
        };
        let indexed = self
            .session
            .find_id(IdKind::RunId, &run.run_id)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let start = indexed
            .iter()
            .find_map(|(seq, record)| matches!(record, Record::RunStart(_)).then_some(*seq))
            .ok_or("sign-in pause has no run start")?;
        let records = self
            .session
            .records_from(start)
            .await
            .map_err(|error| format!("{error:?}"))?;
        Ok(records.iter().rev().find_map(|(seq, record)| match record {
            Record::Pause(pause)
                if pause.run_id == run.run_id && pause.reason == PauseReason::AuthRequired =>
            {
                Some(TimedPause {
                    run_id: RunId(run.run_id.clone()),
                    pause_seq: *seq,
                    resume_not_before_ms: 0,
                    sign_in: true,
                })
            }
            _ => None,
        }))
    }

    pub(super) async fn sign_in_recheck_mode(&mut self) -> StoreRunMode {
        let pause = match self.current_sign_in().await {
            Ok(Some(pause)) => pause,
            Ok(None) => return StoreRunMode::Resume,
            Err(error) => {
                tracing::warn!(target: "broca.store_actor", %error, "sign-in pause re-check could not read its durable boundary");
                return StoreRunMode::Resume;
            }
        };
        if self.sign_in_in_flight.as_ref().is_some_and(|reserved| {
            reserved.run_id == pause.run_id && reserved.pause_seq == pause.pause_seq
        }) {
            return StoreRunMode::Resume;
        }
        let ask = match &self.sign_in_check {
            Some(check)
                if check.pause.run_id == pause.run_id
                    && check.pause.pause_seq == pause.pause_seq =>
            {
                check.may_ask((self.now_ms)())
            }
            _ => true,
        };
        if ask {
            self.sign_in_in_flight = Some(pause);
            StoreRunMode::RecheckSignIn
        } else {
            StoreRunMode::Resume
        }
    }

    pub(super) async fn finish_sign_in_check(
        &mut self,
        outcome: Option<broca_session_loop::account_move::SignInOutcome>,
    ) {
        use broca_session_loop::account_move::SignInOutcome;
        let reservation = self.sign_in_in_flight.take();
        if reservation.is_none() && outcome.is_none() {
            return;
        }
        let Ok(Some(pause)) = self.current_sign_in().await else {
            return;
        };
        if let Some(outcome) = outcome {
            self.sign_in_check = Some(SignInCheck {
                pause,
                outcome,
                failed_checks: 0,
                retry_not_before_ms: 0,
            });
        } else if reservation.as_ref().is_some_and(|reserved| {
            reserved.run_id == pause.run_id && reserved.pause_seq == pause.pause_seq
        }) {
            if let Some(check) = &mut self.sign_in_check {
                if check.pause.run_id == pause.run_id
                    && check.pause.pause_seq == pause.pause_seq
                    && matches!(
                        check.outcome,
                        SignInOutcome::Hold {
                            until_ms: Some(_),
                            ..
                        }
                    )
                {
                    // Keep routing's hold when admission fails, no question can be sent, or
                    // an offered credential is refused locally. Back off from 30 seconds to
                    // at most ten minutes without recording a new routing answer.
                    check.failed_checks = check.failed_checks.saturating_add(1);
                    let delay = (30_000u64 << (check.failed_checks - 1).min(5)).min(600_000);
                    check.retry_not_before_ms = (self.now_ms)().saturating_add(delay);
                }
            }
        }
    }

    /// Reports the session's run to diagnostics when it opens paused for sign-in or an account
    /// limit. Only here can a pause from before this process started be reported: its record
    /// was written before this process began observing the session's records.
    pub(super) async fn report_open_pause(&self) {
        match crate::run_outcomes::paused_at_open(&self.session).await {
            Ok(Some(paused)) => crate::diagnostics::RUN_OUTCOMES.record(paused),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(target: "broca.store_actor", %error, "a paused run could not be reported to diagnostics")
            }
        }
    }

    pub(super) async fn probe_sign_in_on_open(&mut self) {
        if !self.sign_in_open_owed {
            return;
        }
        if self
            .sign_in_open_deadline
            .is_none_or(|deadline| tokio::time::Instant::now() >= deadline)
        {
            self.sign_in_open_owed = false;
            self.sign_in_open_deadline = None;
            return;
        }
        if self.run.is_some()
            || self.stopping
            || self
                .registry
                .as_ref()
                .is_some_and(|registry| !registry.restart_resume_ready() || registry.is_stopping())
        {
            return;
        }
        self.sign_in_open_owed = false;
        self.sign_in_open_deadline = None;
        let Some(run) =
            self.session.operations().run.as_ref().filter(|run| {
                run.finished.is_none() && run.paused == Some(PauseReason::AuthRequired)
            })
        else {
            return;
        };
        let start = match self.recovery_start(run) {
            Ok(start) => start,
            Err(error) => {
                tracing::warn!(target: "broca.store_actor", %error, "sign-in probe could not read its frozen start");
                return;
            }
        };
        if self.sign_in_recheck_mode().await == StoreRunMode::RecheckSignIn {
            self.start_run(start, None, None, StoreRunMode::ProbeSignIn)
                .await;
        }
    }

    /// Ends a timed account-limit resume that could not continue. `class` is the failure's own
    /// class, so a dependency that was briefly unavailable is reported as transient rather than
    /// as a fault that sending again would meet.
    pub(super) async fn refuse_account_limit_resume(
        &mut self,
        run_id: &RunId,
        class: ErrorClass,
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
            class,
            message,
            provider_code: Some("account_limit_resume_refused".into()),
            status: None,
            retry_after_secs: None,
            cause: None,
            provider_subcode: None,
            refusal: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            provider_details: None,
            account_continue: None,
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
