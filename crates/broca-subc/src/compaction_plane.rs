//! Compaction-provider RPCs on the same module-level role routes as step-transform hooks.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    compaction::COMPACTION_CALL_BUDGET_MS,
    compaction_provider::{
        describe::check_describe,
        errors::KnownCode,
        ops,
        ride::{RideAnswer, RideRequest},
        setup::{SetupAnswer, SetupRequest},
        view::{ViewGetAnswer, ViewGetRequest},
        OpRequest, ROLE, VERSION,
    },
    CompactionPlane, CompactionUnavailable,
};
use broca_fetch_plan::{DescribeAnswer, FetchError};
use broca_protocol::BindIdentity;
use tokio::time::Instant;

use crate::{connection::SubcConnection, role_route::RoleRoute};

/// Routes are isolated by caller session and provider. Discovery, queueing and the RPC all
/// consume the call's budget; failed calls invalidate the route before another request uses it.
pub struct SubcCompactionPlane {
    route: RoleRoute,
}

impl SubcCompactionPlane {
    pub fn new(conn: Arc<SubcConnection>) -> Self {
        Self {
            route: RoleRoute::new(conn, ROLE, VERSION, |describe| {
                check_describe(describe)
                    .map(|_| ())
                    .map_err(|problems| format!("{problems:?}"))
            }),
        }
    }

    /// Setup configuration and composition pass through unchanged. Request identity must match
    /// the caller's durable session key, not the module route's synthetic `runner` harness.
    pub async fn setup_with_deadline(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: SetupRequest,
        deadline: Instant,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        if request.session != bind.session || request.harness != bind.harness {
            return Err(CompactionUnavailable);
        }
        self.route
            .call(
                provider,
                bind,
                &OpRequest::new(ops::COMPACTION_SETUP, &request),
                bounded(deadline),
                false,
            )
            .await
            .map_err(|_| CompactionUnavailable)
    }

    pub async fn close_session_routes(&self, bind: &BindIdentity) {
        self.route.close_session_routes(bind).await;
    }

    /// `provider`'s answer to `role.describe`, asked on a fresh `compaction-provider: v1`
    /// route that is closed again before this returns. Fetch-plan admission calls this to learn
    /// whether a plan's compaction provider serves the role before it admits the session's
    /// first send. `compaction.setup` is not sent, and the cached
    /// routes those calls reuse are left alone.
    ///
    /// A refusal of the describe is the provider saying it does not serve the role
    /// ([`DescribeAnswer::Refused`]) unless `describe_refusal_may_clear` or
    /// `describe_refusal_is_routing` says otherwise; those are errors, so admission reports
    /// them as `fetch_unavailable`.
    pub async fn describe(
        &self,
        provider: &str,
        bind: &BindIdentity,
        deadline: Instant,
    ) -> Result<DescribeAnswer, FetchError> {
        match self.route.describe(provider, bind, deadline).await? {
            Ok(answer) => Ok(DescribeAnswer::Answered(answer)),
            Err(refusal) => {
                let may_clear = describe_refusal_may_clear(&refusal.code);
                if may_clear || describe_refusal_is_routing(&refusal.code) {
                    Err(FetchError::Refused {
                        code: refusal.code,
                        detail: refusal.detail,
                        retryable: may_clear,
                    })
                } else {
                    Ok(DescribeAnswer::Refused { code: refusal.code })
                }
            }
        }
    }
}

/// Whether a refusal of `role.describe` may clear if asked again: a code the compaction role
/// marks retryable (`transient`, `provider_busy`, ...), or a code the subc daemon answers in
/// the module's place while the route or module is reloading, warming or otherwise briefly
/// unavailable, or the route has gone stale. Admission retries these until its deadline.
pub(crate) fn describe_refusal_may_clear(code: &str) -> bool {
    use subc_protocol::error_codes as subc;
    KnownCode::from_code(code).is_some_and(KnownCode::retryable)
        || subc::is_retryable_route_open(code)
        || matches!(code, subc::UNKNOWN_CHANNEL | subc::STALE_ROUTE_EPOCH)
}

/// Whether a refusal of `role.describe` is the subc daemon reporting that the module is not
/// there to ask (unregistered, removed, or speaking no protocol). That is the daemon's answer,
/// not the provider's, so it says nothing about the role: admission reports it as
/// `fetch_unavailable` without retrying, as it does a missing provider on route open.
pub(crate) fn describe_refusal_is_routing(code: &str) -> bool {
    use subc_protocol::error_codes as subc;
    matches!(
        code,
        subc::UNKNOWN_MODULE | subc::MODULE_REMOVED | subc::MODULE_NO_PROTOCOL
    )
}

fn bounded(deadline: Instant) -> Instant {
    deadline.min(Instant::now() + Duration::from_millis(COMPACTION_CALL_BUDGET_MS))
}

#[async_trait]
impl CompactionPlane for SubcCompactionPlane {
    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        self.setup_with_deadline(
            provider,
            bind,
            request,
            bounded(Instant::now() + Duration::from_millis(COMPACTION_CALL_BUDGET_MS)),
        )
        .await
    }

    /// `view.get` on the session's compaction route, within the call budget. A request for a
    /// session other than the caller's is never sent.
    async fn view_get(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        if request.session_ref != broca_core::wal_writer::agent_session_ref(bind) {
            return Err(CompactionUnavailable);
        }
        self.route
            .call(
                provider,
                bind,
                &OpRequest::new(ops::VIEW_GET, &request),
                bounded(Instant::now() + Duration::from_millis(COMPACTION_CALL_BUDGET_MS)),
                false,
            )
            .await
            .map_err(|_| CompactionUnavailable)
    }

    /// `compaction.ride` on the session's compaction route, waiting no longer than the
    /// request's own `deadline_ms` (nor the call budget). A request for a session other than
    /// the caller's is never sent.
    async fn ride(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: RideRequest,
    ) -> Result<RideAnswer, CompactionUnavailable> {
        if request.session_ref != broca_core::wal_writer::agent_session_ref(bind) {
            return Err(CompactionUnavailable);
        }
        let ride_deadline = Instant::now() + Duration::from_millis(request.deadline_ms);
        self.route
            .call(
                provider,
                bind,
                &OpRequest::new(ops::COMPACTION_RIDE, &request),
                bounded(ride_deadline),
                false,
            )
            .await
            .map_err(|_| CompactionUnavailable)
    }
}

#[cfg(test)]
mod describe_refusal_tests {
    use super::*;

    #[test]
    fn only_codes_that_may_clear_or_that_the_daemon_answers_are_not_a_provider_saying_no() {
        for code in [
            "transient",
            "provider_busy",
            "history_unreadable",
            "module_reloading",
            "module_warming",
            "target_unavailable",
            "module_timeout",
            "unknown_channel",
            "stale_route_epoch",
        ] {
            assert!(describe_refusal_may_clear(code), "{code}");
            assert!(!describe_refusal_is_routing(code), "{code}");
        }
        for code in ["unknown_module", "module_removed", "module_no_protocol"] {
            assert!(!describe_refusal_may_clear(code), "{code}");
            assert!(describe_refusal_is_routing(code), "{code}");
        }
        // The provider's own definitive answers, including codes no role names.
        for code in [
            "unknown_method",
            "invalid_request",
            "invalid_params",
            "misconfigured",
            "some_future_code",
        ] {
            assert!(!describe_refusal_may_clear(code), "{code}");
            assert!(!describe_refusal_is_routing(code), "{code}");
        }
    }
}
