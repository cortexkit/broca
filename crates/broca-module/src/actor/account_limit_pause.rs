use super::*;

/// A rebuildable scheduling hint. Only a matching WAL pause can authorize a wake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TimedPause {
    pub run_id: RunId,
    pub pause_seq: u64,
    pub resume_not_before_ms: u64,
}

fn current(frames: &[broca_wal::FramedRecord]) -> Result<Option<TimedPause>, String> {
    let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
    let prior = broca_core::replay(&records).map_err(|error| error.to_string())?;
    let broca_core::ReplayResolution::AuthRequired {
        run_id,
        reason: broca_protocol::PauseReason::AccountLimit,
        error,
        pending: None,
    } = prior.resolution
    else {
        return Ok(None);
    };
    if prior.cancel_at.is_some() {
        return Ok(None);
    }
    let pause = prior
        .account_limit_pause
        .ok_or("account-limit pause has no recovery evidence")?;
    if error
        .as_ref()
        .is_none_or(|error| error.class != ErrorClass::Transient)
    {
        return Err("account-limit pause does not carry a transient source error".into());
    }
    let selection = prior
        .run_config
        .as_ref()
        .and_then(|config| config.auth_selection.as_ref())
        .ok_or("account-limit pause has no frozen credential selection")?;
    if selection.credential_id != pause.credential_id {
        return Err("account-limit pause does not match the frozen credential selection".into());
    }
    let pause_seq = frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.record {
            WalRecord::RunPaused {
                run_id: paused,
                reason: broca_protocol::PauseReason::AccountLimit,
                ..
            } if paused == &run_id => Some(frame.seq),
            _ => None,
        })
        .ok_or("account-limit pause has no WAL sequence")?;
    let matched_attempt = frames.iter().any(|frame| {
        if frame.seq != pause.failed_attempt_seq || frame.seq >= pause_seq {
            return false;
        }
        matches!(&frame.record, WalRecord::ModelAttemptFinished {
            account_limit_disposition: Some(disposition), step_id, attempt,
            request_body_len, request_body_sha256, attempt_id, ..
        } if *disposition == pause.disposition && *step_id == pause.failed_step
            && *attempt == pause.attempt && *request_body_len == pause.request_body_len
            && *request_body_sha256 == pause.request_body_sha256
            && pause.attempt_id.as_ref().is_none_or(|id| Some(id) == attempt_id.as_ref()))
    });
    if !matched_attempt {
        return Err(
            "account-limit pause does not reference its source-classified failed attempt".into(),
        );
    }
    Ok(Some(TimedPause {
        run_id,
        pause_seq,
        resume_not_before_ms: pause.resume_not_before_ms,
    }))
}

impl<W: WalStore + 'static> LineageActor<W> {
    pub(super) async fn arm_account_limit(&mut self) -> Result<(), String> {
        self.account_limit_timer = current(&self.read_wal_framed_records().await?)?;
        Ok(())
    }

    pub(super) async fn wake_account_limit(&mut self, expected: TimedPause) -> Result<(), String> {
        if !matches!(self.state, RunState::Idle) || self.registry.is_stopping() {
            return Ok(());
        }
        self.account_limit_timer = None;
        self.refresh_admission_view().await?;
        let Some(pause) = current(&self.read_wal_framed_records().await?)? else {
            return Ok(());
        };
        let now = self
            .registry
            .retention_service()
            .map_or_else(epoch_ms, |service| service.clock.now_ms());
        if pause != expected
            || now < pause.resume_not_before_ms
            || !self.registry.restart_resume_ready()
        {
            self.account_limit_timer = Some(pause);
            return Ok(());
        }
        let episode = self
            .prior_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.checkpoint_at_head.last_episode.as_ref())
            .filter(|episode| {
                episode.run_id == pause.run_id
                    && episode.disposition == EpisodeDisposition::Paused
                    && episode.pause_reason == Some(broca_protocol::PauseReason::AccountLimit)
            })
            .cloned()
            .ok_or("account-limit wake no longer owns the paused episode")?;
        let mut request = submission_from_episode(&episode);
        request.keep_warm = episode.keep_warm;
        request.on_restart = episode.on_restart;
        match self
            .start_run(
                pause.run_id,
                request,
                RunKind::FreshOrResume,
                None,
                SendIdentity {
                    title: None,
                    conversation_key: None,
                    send_id: None,
                    request_identity_digest: episode.request_identity_digest,
                },
                false,
            )
            .await
        {
            Ok(_) | Err(StartRunFailure::Shutdown) => Ok(()),
            Err(error) if self.nested_exit_requested || self.writer_poisoned => {
                Err(error.to_string())
            }
            Err(error) => {
                let refusal = ProviderError {
                    class: ErrorClass::Permanent,
                    message: error.to_string(),
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
                self.write_interrupted_seal_with_error(None, SealCause::CrashOrphan, Some(refusal))
                    .await?;
                self.refresh_admission_view().await?;
                self.drain_queue_one().await
            }
        }
    }
}
