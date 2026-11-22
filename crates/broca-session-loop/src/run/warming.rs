//! A run's keep-warm while it waits on tool calls: which runs warm, the scheduler's view of each
//! wait, and what a wait resumed after a restart already sent.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{Admission, Seams};

/// A run's keep-warm: the scheduler warms the prompt cache while the run waits on tool
/// calls, through the driver the caller gave, for a run whose keep-warm is not off.
pub(crate) struct Warming<'a> {
    driver: &'a Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<Admission>>,
    admission: &'a Admission,
    keep_warm: &'a broca_protocol::request::KeepWarm,
    run_id: broca_protocol::RunId,
    /// Whether the run already logged that its cache can't be warmed, so later waits don't
    /// repeat it.
    unsupported_logged: std::sync::atomic::AtomicBool,
    /// A warm that missed the cache stops warming for the rest of the run.
    warm_missed: std::sync::atomic::AtomicBool,
}

impl<'a> Warming<'a> {
    /// The run's keep-warm, if it asked for one and the caller gave a driver.
    pub(crate) fn of<P, T>(admission: &'a Admission, seams: &'a Seams<'_, P, T>) -> Option<Self> {
        let keep_warm = admission
            .keep_warm
            .as_ref()
            .filter(|keep_warm| keep_warm.warms())?;
        Some(Self {
            driver: seams.keep_warm.as_ref()?,
            admission,
            keep_warm,
            run_id: broca_protocol::RunId(admission.run_id.clone()),
            unsupported_logged: Default::default(),
            warm_missed: Default::default(),
        })
    }

    /// The scheduler's view of the wait, which `sent` warms already went out for.
    pub(crate) fn wait<'w>(
        &'w self,
        cancel: &'w CancellationToken,
        sent: u32,
    ) -> broca_core::keep_warm_wait::WarmWait<'w, Admission> {
        broca_core::keep_warm_wait::WarmWait {
            driver: self.driver,
            config: self.admission,
            run_id: &self.run_id,
            cancel: Some(cancel),
            unsupported_logged: &self.unsupported_logged,
            warm_missed: &self.warm_missed,
            keep_warm: self.keep_warm,
            warms_before: sent,
        }
    }

    /// What the run's keep-warm already spent warming during the wait after step `step_id`.
    pub(crate) fn spent(
        &self,
        operations: &broca_session_store::fold::operations::Operations,
        step_id: u64,
    ) -> f64 {
        warm_spend(self.driver.as_ref(), self.admission, operations, step_id)
    }
}

/// How many warms a run's keep-warm already sent during the wait after step `step_id`, which
/// count toward the cap of `always` and `hours`.
pub fn warm_count(
    operations: &broca_session_store::fold::operations::Operations,
    step_id: u64,
) -> u32 {
    operations.run.as_ref().map_or(0, |run| {
        let sent = run
            .warms
            .iter()
            .filter(|warm| warm.step_id == step_id)
            .count();
        u32::try_from(sent).unwrap_or(u32::MAX)
    })
}

/// What a run's keep-warm already spent warming during the wait after step `step_id`, priced
/// after a restart by `broca_core::keep_warm_wait::batch_spend`: a warm that reached the cache
/// at its recorded price, a failed one that was sent and reported usage at its measured price,
/// any other at nothing. A warm the driver can't price counts as unaffordable, so the wait
/// sends no more.
pub fn warm_spend<C: ?Sized + Sync>(
    driver: &dyn broca_core::keep_warm_wait::KeepWarmDriver<C>,
    config: &C,
    operations: &broca_session_store::fold::operations::Operations,
    step_id: u64,
) -> f64 {
    let Some(run) = operations.run.as_ref() else {
        return 0.0;
    };
    run.warms
        .iter()
        .filter(|warm| warm.step_id == step_id)
        .map(|warm| {
            let billing = |key: &str| warm.billing.as_ref().and_then(|billing| billing.get(key));
            let envelope: Option<broca_records::record::AttemptEnvelope> = billing("envelope")
                .and_then(|envelope| serde_json::from_value(envelope.clone()).ok());
            if warm.reached_cache {
                return driver
                    .recorded_cost(config, &warm.usage)
                    .unwrap_or(f64::INFINITY);
            }
            match envelope {
                Some(envelope) if broca_core::cache_warm::warm_budget_needs_pricing(&envelope) => {
                    let reservation: Option<broca_records::record::AttemptReservation> =
                        billing("reservation").and_then(|reservation| {
                            serde_json::from_value(reservation.clone()).ok()
                        });
                    match reservation {
                        Some(reservation) => driver
                            .failed_cost(config, &reservation, &envelope, warm.ttl_ms)
                            .unwrap_or(f64::INFINITY),
                        None => f64::INFINITY,
                    }
                }
                _ => 0.0,
            }
        })
        .sum()
}
