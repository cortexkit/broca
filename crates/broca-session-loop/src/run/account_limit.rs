use super::{run_selection, run_state, Admission, LoopError, Seams, Writer};
use broca_core::account_limit_pause::{FailedAccountLimitAttempt, PauseBudget};
use broca_core::provider::Provider;
use broca_protocol::{PauseReason, ProviderError, StepId};
use broca_records::record::AccountLimitPause;
use broca_session_store::record::Record;

/// The timed account-limit pause the run's state holds, if it is paused on one.
pub(super) fn account_limit_of<W: Writer>(
    writer: &W,
) -> Result<Option<AccountLimitPause>, LoopError> {
    run_state(writer)
        .account_limit_pause
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| LoopError::Parts(format!("account-limit pause: {e}")))
}

/// The pause records its source step, so resuming or restarting does not renew its wait.
pub(super) async fn waited_for_routing<W: Writer>(
    writer: &W,
    run_id: &str,
    step_id: u64,
    credential: Option<&str>,
) -> Result<bool, LoopError> {
    Ok(writer
        .open_log_records()
        .await?
        .iter()
        .rev()
        .any(|(_, record)| {
            let Record::Pause(pause) = record else {
                return false;
            };
            if pause.run_id != run_id || pause.reason != PauseReason::AccountLimit {
                return false;
            }
            let Some(limit) = pause.account_limit.as_ref().map(|limit| limit.parse()) else {
                return false;
            };
            limit["failed_step"].as_u64() == Some(step_id)
                && credential.is_none_or(|id| limit["credential_id"].as_str() == Some(id))
                && pause.error.as_ref().is_some_and(|error| {
                    let error = error.parse();
                    error["account_continue"]["routing_code"] == "no_route"
                        && error["account_continue"]["reason"] == "concurrency_full"
                })
        }))
}

/// A wait for the spent account's own reset requires consent at admission and a reset within
/// both the boot cap and the remaining lifetime of that account's cache.
pub(super) fn account_limit_pause<P: Provider, T>(
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    step_id: u64,
    limited: &FailedAccountLimitAttempt,
    error: &ProviderError,
) -> Option<Box<AccountLimitPause>> {
    let selection = run_selection(seams, admission);
    let last_accepted = limited
        .attempt
        .served_account_ref
        .as_deref()
        .or_else(|| selection.and_then(|selection| selection.account_ref.as_deref()))
        .and_then(|account| seams.provider.last_accepted_at_ms(account));
    let budget = PauseBudget {
        setting: if admission.account_limit_pause {
            seams.max_same_account_wait
        } else {
            broca_core::SameAccountWait::Minutes(0)
        },
        cache_expiry_ms: broca_core::session_account_cache_expiry_ms(
            last_accepted,
            &admission.render_config,
        ),
    };
    limited.pause(StepId(step_id), (seams.now_ms)(), error, selection, &budget)
}
