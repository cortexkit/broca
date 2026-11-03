//! Shared module-level role RPCs: frozen session scopes, discovery, caching and deadline cleanup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_fetch_plan::FetchError;
use broca_protocol::{BindIdentity, SessionScope};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use subc_protocol::scope::ScopeSelector;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};
use tokio::time::{timeout_at, Instant};

use crate::connection::{ConnError, RouteHandle, RouteOpenRetry, SubcConnection};

// The serialized selector includes its owner, ref and frozen epoch. Keeping it in the key
// prevents a route opened for one scope from serving another, even for the same bind identity.
type RouteKey = (BindIdentity, String, Option<String>);
type RouteSlot = Arc<AsyncMutex<Option<RouteHandle>>>;

fn route_key(bind: &BindIdentity, provider: &str, selector: Option<&ScopeSelector>) -> RouteKey {
    (
        bind.clone(),
        provider.to_owned(),
        selector.map(|selector| {
            serde_json::to_string(selector).expect("scope selector is serializable")
        }),
    )
}

type DescribeCheck = fn(&Value) -> Result<(), String>;

pub(crate) struct RoleRoute {
    conn: Arc<SubcConnection>,
    role: &'static str,
    version: &'static str,
    check_describe: Option<DescribeCheck>,
    routes: Mutex<HashMap<RouteKey, RouteSlot>>,
}

impl RoleRoute {
    pub(crate) fn new(
        conn: Arc<SubcConnection>,
        role: &'static str,
        version: &'static str,
        check_describe: DescribeCheck,
    ) -> Self {
        Self {
            conn,
            role,
            version,
            check_describe: Some(check_describe),
            routes: Mutex::new(HashMap::new()),
        }
    }

    /// Protocols whose role contract has no discovery op open their bound route
    /// directly. Role selection is still enforced by the daemon at route open.
    pub(crate) fn without_discovery(
        conn: Arc<SubcConnection>,
        role: &'static str,
        version: &'static str,
    ) -> Self {
        Self {
            conn,
            role,
            version,
            check_describe: None,
            routes: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn close_session_routes(&self, bind: &BindIdentity) {
        let slots: Vec<_> = {
            let mut routes = self.routes.lock().unwrap();
            let keys: Vec<_> = routes
                .keys()
                .filter(|(key, _, _)| key == bind)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| routes.remove(&key))
                .collect()
        };
        for slot in slots {
            if let Some(route) = slot.lock().await.take() {
                let _ = self.conn.route_close(&route).await;
            }
        }
    }

    pub(crate) async fn call<P: Serialize + Sync, A: DeserializeOwned>(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        request: &P,
        deadline: Instant,
        retry_transient: bool,
    ) -> Result<A, FetchError> {
        let body =
            serde_json::to_vec(request).map_err(|error| FetchError::Lost(error.to_string()))?;
        timeout_at(deadline, async {
            let selector = self.selector(scope).await?;
            let slot = self
                .routes
                .lock()
                .unwrap()
                .entry(route_key(bind, provider, selector.as_ref()))
                .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
                .clone();
            let mut guard = slot.lock().await;
            let mut retried = false;
            loop {
                if Instant::now() >= deadline {
                    return Err(expired());
                }
                let result = self
                    .attempt(
                        provider,
                        bind,
                        selector.clone(),
                        &body,
                        deadline,
                        &mut guard,
                    )
                    .await;
                if retry_transient
                    && !retried
                    && matches!(
                        &result,
                        Err(
                            FetchError::Refused { code, .. }
                            | FetchError::ProviderRefused { code, .. }
                        ) if code == "transient"
                    )
                {
                    retried = true;
                    continue;
                }
                return result;
            }
        })
        .await
        .unwrap_or_else(|_| Err(expired()))
    }

    async fn attempt<A: DeserializeOwned>(
        &self,
        provider: &str,
        bind: &BindIdentity,
        selector: Option<ScopeSelector>,
        body: &[u8],
        deadline: Instant,
        guard: &mut MutexGuard<'_, Option<RouteHandle>>,
    ) -> Result<A, FetchError> {
        // Only a decoded answer returns the handle to the cache. Errors and cancelled futures
        // close their lease, so subsequent calls rediscover instead of reusing a poisoned route.
        let mut lease = RouteLease {
            conn: self.conn.clone(),
            route: guard.take(),
        };
        if lease
            .route
            .is_some_and(|route| !self.conn.route_is_open(&route))
        {
            lease.route = None;
        }
        if lease.route.is_none() {
            if let Some(check_describe) = self.check_describe {
                let (opened, describe) = self
                    .open_and_describe(provider, bind, selector, deadline)
                    .await?;
                lease = opened;
                let describe = describe.map_err(refused)?;
                check_describe(&describe).map_err(|problems| FetchError::Refused {
                    code: broca_fetch_plan::admit::INVALID_ANSWER.into(),
                    detail: Some(serde_json::json!({"provider": provider, "problems": problems})),
                    retryable: false,
                })?;
            } else {
                lease = self.open(provider, bind, selector, deadline).await?;
            }
        }
        let answer = self
            .request(lease.route.as_ref().unwrap(), body.to_vec(), deadline)
            .await?;
        **guard = lease.route.take();
        Ok(answer)
    }

    /// One discovery on a fresh route: open it and return its unchecked `role.describe` answer,
    /// or the provider's refusal of the describe as it sent it, without sending any op of the
    /// role; then close the route. Fetch-plan admission uses this to learn whether a provider
    /// serves the role before it admits a session's first send, so a session is never started
    /// against a provider that cannot serve it. It never touches the cached call routes.
    pub(crate) async fn describe(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        deadline: Instant,
    ) -> Result<Result<Value, subc_protocol::ErrorBody>, FetchError> {
        timeout_at(deadline, async {
            let selector = self.selector(scope).await?;
            let (mut lease, describe) = self
                .open_and_describe(provider, bind, selector, deadline)
                .await?;
            if let Some(route) = lease.route.take() {
                let _ = self.conn.route_close(&route).await;
            }
            Ok(describe)
        })
        .await
        .unwrap_or_else(|_| Err(expired()))
    }

    /// Open a route to `provider` under this role's `role_versions` and ask it `role.describe`.
    /// The returned lease owns the route; dropping it closes the route. A refusal of the
    /// describe is returned as sent, for the caller to classify.
    async fn open_and_describe(
        &self,
        provider: &str,
        bind: &BindIdentity,
        selector: Option<ScopeSelector>,
        deadline: Instant,
    ) -> Result<(RouteLease, Result<Value, subc_protocol::ErrorBody>), FetchError> {
        let lease = self.open(provider, bind, selector, deadline).await?;
        let describe = match self
            .exchange(
                lease.route.as_ref().expect("opened route"),
                serde_json::to_vec(&serde_json::json!({"method": "role.describe", "params": {}}))
                    .expect("describe request is serializable"),
                deadline,
            )
            .await?
        {
            Ok(reply) => Ok(decode(&reply)?),
            Err(refusal) => Err(refusal),
        };
        Ok((lease, describe))
    }

    async fn open(
        &self,
        provider: &str,
        bind: &BindIdentity,
        selector: Option<ScopeSelector>,
        deadline: Instant,
    ) -> Result<RouteLease, FetchError> {
        let scoped = selector.is_some();
        let route = self
            .conn
            .route_open_role_within(
                provider,
                bind,
                self.role,
                self.version,
                selector,
                RouteOpenRetry {
                    budget: deadline.saturating_duration_since(Instant::now()),
                    max_interval: Duration::from_secs(1),
                },
            )
            .await
            .map_err(|error| {
                if scoped {
                    crate::role_route_diagnostics::observe(provider, self.role, bind, &error);
                }
                open_error(error)
            })?;
        Ok(RouteLease {
            conn: self.conn.clone(),
            route: Some(route),
        })
    }

    async fn selector(
        &self,
        scope: Option<&SessionScope>,
    ) -> Result<Option<ScopeSelector>, FetchError> {
        let Some(scope) = scope else {
            return Ok(None);
        };
        let capabilities = self
            .conn
            .server_capabilities()
            .await
            .map_err(|error| FetchError::Lost(error.to_string()))?;
        if !capabilities
            .iter()
            .any(|capability| capability == subc_protocol::scope::CAP_SCOPES_V1)
        {
            return Err(FetchError::Refused {
                code: "scope_unsupported".into(),
                detail: None,
                retryable: false,
            });
        }
        crate::subc_plane::frozen_scope_selector(scope)
            .map(Some)
            .map_err(|detail| FetchError::Refused {
                code: "scope_unverifiable".into(),
                detail: Some(Value::String(detail)),
                retryable: false,
            })
    }

    async fn request<A: DeserializeOwned>(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<A, FetchError> {
        match self.exchange(route, body, deadline).await? {
            Ok(reply) => decode(&reply),
            Err(refusal) => Err(refused(refusal)),
        }
    }

    /// One request on `route`: the reply bytes, or the error body the route answered with.
    async fn exchange(
        &self,
        route: &RouteHandle,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<Result<Vec<u8>, subc_protocol::ErrorBody>, FetchError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        self.conn
            .route_unary_reply(route, body, remaining)
            .await
            .map_err(|error| FetchError::Lost(error.to_string()))
    }
}

fn decode<A: DeserializeOwned>(reply: &[u8]) -> Result<A, FetchError> {
    serde_json::from_slice(reply).map_err(|error| {
        FetchError::provider_refused(
            broca_fetch_plan::admit::INVALID_ANSWER.into(),
            None,
            &format!("provider role reply is not valid JSON: {error}"),
            false,
        )
    })
}

/// A role call's refusal; only `transient` is retried.
fn refused(refusal: subc_protocol::ErrorBody) -> FetchError {
    let retryable = refusal.code == "transient";
    FetchError::provider_refused(refusal.code, refusal.detail, &refusal.message, retryable)
}

struct RouteLease {
    conn: Arc<SubcConnection>,
    route: Option<RouteHandle>,
}
impl Drop for RouteLease {
    fn drop(&mut self) {
        if let Some(route) = self.route.take() {
            let conn = self.conn.clone();
            // Closing a known handle must not extend the RPC deadline. Opens cancelled before
            // returning a handle are covered by the connection's late-open cleanup.
            tokio::spawn(async move {
                let _ = conn.route_close(&route).await;
            });
        }
    }
}

fn expired() -> FetchError {
    FetchError::Lost("role call deadline expired".into())
}
/// Role and tool-provider admission opens share the protocol's retry allowlist.
pub(crate) fn open_error(error: ConnError) -> FetchError {
    match error {
        ConnError::Rejected(message) => {
            let code = message.code();
            if code == subc_protocol::error_codes::UNKNOWN_MODULE {
                FetchError::UnknownModule
            } else {
                FetchError::Refused {
                    code: code.into(),
                    detail: None,
                    retryable: subc_protocol::error_codes::is_retryable_route_open(code),
                }
            }
        }
        error => FetchError::Lost(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_includes_scope_owner_ref_and_frozen_epoch() {
        let bind = BindIdentity::new("/project", "broca", "session");
        let selector = |owner: &str, reference: &str, epoch| {
            ScopeSelector::new(
                subc_protocol::Principal::Reserved {
                    module_id: owner.into(),
                },
                reference,
                epoch,
            )
        };
        let original = route_key(
            &bind,
            "provider",
            Some(&selector("owner", "scope", Some(1))),
        );
        assert_eq!(
            original,
            route_key(
                &bind,
                "provider",
                Some(&selector("owner", "scope", Some(1)))
            )
        );
        for other in [
            None,
            Some(selector("other", "scope", Some(1))),
            Some(selector("owner", "other", Some(1))),
            Some(selector("owner", "scope", Some(2))),
            Some(selector("owner", "scope", None)),
        ] {
            assert_ne!(original, route_key(&bind, "provider", other.as_ref()));
        }
    }

    #[test]
    fn admission_opens_use_the_protocol_retry_allowlist() {
        for (code, retryable) in [
            ("module_warming", true),
            ("module_reloading", true),
            ("target_unavailable", true),
            ("module_timeout", true),
            ("unknown_module", false),
            ("module_rejected", false),
            ("permission_denied", false),
            ("scope_not_carrier", false),
            ("future_code", false),
        ] {
            let body = subc_protocol::ErrorBody::new(code, "module_warming — diagnostic only");
            let error = ConnError::Rejected(crate::connection::decode_error_body(
                &serde_json::to_vec(&body).unwrap(),
            ));
            match open_error(error) {
                FetchError::UnknownModule => assert_eq!(code, "unknown_module"),
                FetchError::Refused {
                    code: actual,
                    retryable: actual_retryable,
                    ..
                } => {
                    assert_eq!(actual, code);
                    assert_eq!(actual_retryable, retryable, "{code}");
                }
                other => panic!("unexpected classification for {code}: {other:?}"),
            }
        }
        assert!(matches!(
            open_error(ConnError::Transport("lost".into())),
            FetchError::Lost(_)
        ));
    }
}
