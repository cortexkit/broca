//! Compaction-provider RPCs on the same module-level role routes as step-transform hooks.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
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
    CompactionPlane, CompactionUnavailable, ProviderCapabilities,
};
use broca_fetch_plan::{DescribeAnswer, FetchError};
use broca_protocol::{BindIdentity, SessionScope};
use subc_control::CatalogEntry;
use tokio::time::Instant;

use crate::{
    connection::{ConnError, SubcConnection},
    role_route::RoleRoute,
};

// Leave time for the previously read catalog list to reach Setup before its two-second deadline.
const CATALOG_READ_BUDGET: Duration = Duration::from_secs(1);

#[derive(Default)]
struct CapabilityCatalog {
    last: Mutex<HashMap<String, Vec<String>>>,
}

impl CapabilityCatalog {
    async fn read(
        &self,
        provider: &str,
        read: impl Future<Output = Result<Vec<CatalogEntry>, ConnError>>,
    ) -> ProviderCapabilities {
        let result = tokio::time::timeout(CATALOG_READ_BUDGET, read).await;
        let mut last = self.last.lock().unwrap_or_else(|error| error.into_inner());
        match result {
            Ok(Ok(entries)) => {
                match crate::provider_catalog::find_module(&entries, provider).cloned() {
                    Some(entry) => {
                        let provides = entry
                            .capabilities
                            .map(|caps| caps.provides)
                            .unwrap_or_default();
                        last.insert(provider.to_owned(), provides.clone());
                        ProviderCapabilities::Declared(provides)
                    }
                    None => {
                        last.remove(provider);
                        ProviderCapabilities::NotInCatalog
                    }
                }
            }
            failure => {
                let kept = last.get(provider).cloned();
                tracing::warn!(provider, used_kept_list = kept.is_some(), error = ?failure,
                    "compaction provider catalog read failed");
                kept.map_or(
                    ProviderCapabilities::Unknown,
                    ProviderCapabilities::Declared,
                )
            }
        }
    }
}

// The bound route supplies the full session identity. Accept its historical
// handle, but address the provider using the current folder-independent handle.
fn current_reference(bind: &BindIdentity, supplied: &str) -> Result<String, CompactionUnavailable> {
    let current = broca_core::session_refs::agent_session_ref(bind);
    let legacy = broca_core::session_refs::legacy_session_ref(
        &bind.harness,
        &bind.project_root.to_string_lossy(),
        &bind.session,
    );
    if supplied == current || supplied == legacy {
        Ok(current)
    } else {
        Err(CompactionUnavailable)
    }
}

/// Routes are isolated by caller session, provider and frozen scope. Discovery, queueing and the RPC all
/// consume the call's budget; failed calls invalidate the route before another request uses it.
pub struct SubcCompactionPlane {
    route: RoleRoute,
    v2_route: RoleRoute,
    /// Asks the daemon's catalog what a provider declares; no route is opened for it.
    conn: Arc<SubcConnection>,
    capabilities: CapabilityCatalog,
}

impl SubcCompactionPlane {
    pub fn new(conn: Arc<SubcConnection>) -> Self {
        Self {
            capabilities: CapabilityCatalog::default(),
            conn: conn.clone(),
            v2_route: RoleRoute::without_discovery(conn.clone(), "compaction-provider", "v2"),
            route: RoleRoute::new(conn, ROLE, VERSION, |describe| {
                check_describe(describe)
                    .map(|_| ())
                    .map_err(|problems| format!("{problems:?}"))
            }),
        }
    }

    /// Setup configuration and composition pass through unchanged. Setup's `harness` and
    /// `session` must equal the caller's own bind identity, which is also the identity the
    /// compaction route is opened with (compaction routes do not present the tool-module
    /// `runner` harness). The provider therefore sees one identity for the session, and calls
    /// it makes back about the conversation reach the session itself. A Setup naming another
    /// harness or session is never sent.
    pub async fn setup_with_deadline(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
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
                scope,
                &OpRequest::new(ops::COMPACTION_SETUP, &request),
                bounded(deadline),
                false,
            )
            .await
            .map_err(|error| {
                log_unanswered(provider, ops::COMPACTION_SETUP, &error);
                CompactionUnavailable
            })
    }

    pub async fn close_session_routes(&self, bind: &BindIdentity) {
        self.route.close_session_routes(bind).await;
        self.v2_route.close_session_routes(bind).await;
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
        scope: Option<&SessionScope>,
        deadline: Instant,
    ) -> Result<DescribeAnswer, FetchError> {
        match self.route.describe(provider, bind, scope, deadline).await? {
            Ok(answer) => Ok(DescribeAnswer::Answered(answer)),
            Err(refusal) => {
                let may_clear = describe_refusal_may_clear(&refusal.code);
                if may_clear || describe_refusal_is_routing(&refusal.code) {
                    Err(FetchError::provider_refused(
                        refusal.code,
                        refusal.detail,
                        &refusal.message,
                        may_clear,
                    ))
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

/// Logs a compaction call that broca counts as unanswered, with why: the daemon knew no such
/// module, a refusal on the way (such as a route the daemon would not open yet), or a call lost
/// in transport or out of time. The runtime only sees "no answer" and retries; this line is how
/// a provider that never saw the call can tell a closed route from its own slow start.
fn log_unanswered(provider: &str, method: &str, error: &FetchError) {
    let (kind, code) = match error {
        FetchError::UnknownModule => ("unknown_module", ""),
        FetchError::Refused { code, .. } => ("refused", code.as_str()),
        FetchError::ProviderRefused { code, .. } => ("provider_refused", code.as_str()),
        FetchError::Lost(reason) => ("lost", reason.as_str()),
    };
    tracing::info!(
        target: "broca.compaction",
        provider,
        method,
        kind,
        code,
        "compaction call got no answer"
    );
}

fn bounded(deadline: Instant) -> Instant {
    deadline.min(Instant::now() + Duration::from_millis(COMPACTION_CALL_BUDGET_MS))
}

#[async_trait]
impl CompactionPlane for SubcCompactionPlane {
    /// The provider's `provides` from the daemon's `catalog.list`, read fresh on every call: a
    /// provider declares compaction capabilities there, and Setup reads them each time it is
    /// sent. Only failed or timed-out reads use a previously read list.
    async fn provider_capabilities(&self, provider: &str) -> ProviderCapabilities {
        self.capabilities
            .read(provider, self.conn.catalog_list())
            .await
    }

    async fn call_v2(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        method: &str,
        params: serde_json::Value,
        deadline_ms: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        use broca_core::compaction_v2::{protocol::wire::Refusal, runtime::CallError};
        self.v2_route
            .call(
                provider,
                bind,
                scope,
                &serde_json::json!({"method": method, "params": params}),
                bounded(Instant::now() + Duration::from_millis(deadline_ms)),
                false,
            )
            .await
            .map_err(|error| match error {
                FetchError::ProviderRefused {
                    code,
                    detail,
                    message,
                    ..
                } => CallError::Refused(Refusal::new(&code, message, detail)),
                other => {
                    log_unanswered(provider, method, &other);
                    CallError::Unavailable
                }
            })
    }

    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        self.setup_with_deadline(
            provider,
            bind,
            scope,
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
        scope: Option<&SessionScope>,
        mut request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        request.session_ref = current_reference(bind, &request.session_ref)?;
        self.route
            .call(
                provider,
                bind,
                scope,
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
        scope: Option<&SessionScope>,
        mut request: RideRequest,
    ) -> Result<RideAnswer, CompactionUnavailable> {
        request.session_ref = current_reference(bind, &request.session_ref)?;
        let ride_deadline = Instant::now() + Duration::from_millis(request.deadline_ms);
        self.route
            .call(
                provider,
                bind,
                scope,
                &OpRequest::new(ops::COMPACTION_RIDE, &request),
                bounded(ride_deadline),
                false,
            )
            .await
            .map_err(|_| CompactionUnavailable)
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;

    fn entry(provider: &str, provides: &[&str]) -> CatalogEntry {
        let mut entry = CatalogEntry::new(provider, Vec::new(), Vec::new());
        entry.capabilities =
            Some(serde_json::from_value(serde_json::json!({"provides": provides})).unwrap());
        entry
    }

    async fn failed() -> Result<Vec<CatalogEntry>, ConnError> {
        Err(ConnError::Transport("catalog unavailable".into()))
    }

    #[tokio::test]
    async fn provider_capabilities_keeps_list_only_for_failed_reads_and_the_same_provider() {
        let catalog = CapabilityCatalog::default();
        let declared = ProviderCapabilities::Declared(vec!["compaction-signed-floor/v1".into()]);
        assert_eq!(
            catalog
                .read("magic", async {
                    Ok(vec![entry("magic", &["compaction-signed-floor/v1"])])
                })
                .await,
            declared
        );
        assert_eq!(catalog.read("magic", failed()).await, declared);
        assert_eq!(
            catalog.read("other", failed()).await,
            ProviderCapabilities::Unknown
        );
        // A successful read replaces the kept list, including a declaration with no capabilities.
        assert_eq!(
            catalog
                .read("magic", async { Ok(vec![entry("magic", &[])]) })
                .await,
            ProviderCapabilities::Declared(vec![])
        );
        assert_eq!(
            catalog.read("magic", failed()).await,
            ProviderCapabilities::Declared(vec![])
        );
    }

    #[tokio::test]
    async fn provider_capabilities_failed_first_read_is_unknown() {
        assert_eq!(
            CapabilityCatalog::default().read("magic", failed()).await,
            ProviderCapabilities::Unknown
        );
    }

    #[tokio::test]
    async fn provider_capabilities_successful_removal_clears_the_kept_list() {
        let catalog = CapabilityCatalog::default();
        let _ = catalog
            .read("magic", async {
                Ok(vec![entry("magic", &["compaction-entry-speaks-for/v1"])])
            })
            .await;
        assert_eq!(
            catalog
                .read("magic", async { Ok(vec![entry("other", &[])]) })
                .await,
            ProviderCapabilities::NotInCatalog
        );
        assert_eq!(
            catalog.read("magic", failed()).await,
            ProviderCapabilities::Unknown
        );
    }

    #[tokio::test(start_paused = true)]
    async fn provider_capabilities_timed_out_read_uses_the_kept_list_or_is_unknown() {
        let catalog = CapabilityCatalog::default();
        let declared =
            ProviderCapabilities::Declared(vec!["compaction-entry-speaks-for/v1".into()]);
        assert_eq!(
            catalog
                .read("magic", async {
                    Ok(vec![entry("magic", &["compaction-entry-speaks-for/v1"])])
                })
                .await,
            declared
        );
        assert_eq!(
            catalog.read("magic", std::future::pending()).await,
            declared
        );
        assert_eq!(
            catalog.read("other", std::future::pending()).await,
            ProviderCapabilities::Unknown
        );
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

#[cfg(test)]
mod reference_tests {
    use super::*;

    #[test]
    fn compaction_reference_accepts_v1_and_v2_but_forwards_v2() {
        let bind = BindIdentity::new("/tmp/project", "runner", "s1");
        let v2 = "701a6e2266b2d8d3740373257146e954";
        assert_eq!(current_reference(&bind, v2).unwrap(), v2);
        assert_eq!(
            current_reference(&bind, "fb99bd3a001d05d029c607bd048ed5a1").unwrap(),
            v2
        );
        assert!(current_reference(&bind, "58710a071e777b22bf1b3734b691ef7d").is_err());
        assert!(current_reference(&bind, "unrelated").is_err());
    }
}

#[cfg(test)]
mod unanswered_log_tests {
    use super::*;

    /// Each unanswered call names its provider, method and why it got no answer, so a provider
    /// that never saw a call can tell a closed route from its own slow start.
    #[tokio::test]
    async fn an_unanswered_compaction_call_logs_why() {
        let ((), logs) = broca_test_support::capture_logs(async {
            log_unanswered(
                "magic-context",
                "compaction.setup",
                &FetchError::Refused {
                    code: "module_warming".into(),
                    detail: None,
                    retryable: true,
                },
            );
            log_unanswered(
                "magic-context",
                "compaction.setup",
                &FetchError::Lost("timed out".into()),
            );
        })
        .await;
        let lines: Vec<(String, String, String)> = logs
            .iter()
            .filter(|event| event.target == "broca.compaction")
            .map(|event| {
                let field = |name: &str| event.fields.get(name).cloned().unwrap_or_default();
                (field("provider"), field("kind"), field("code"))
            })
            .collect();
        assert_eq!(
            lines,
            [
                (
                    "magic-context".into(),
                    "refused".into(),
                    "module_warming".into()
                ),
                ("magic-context".into(), "lost".into(), "timed out".into()),
            ]
        );
    }
}
