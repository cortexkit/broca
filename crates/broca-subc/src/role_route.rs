//! Shared module-level role RPCs: unscoped routes, discovery, caching and deadline cleanup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_fetch_plan::FetchError;
use broca_protocol::BindIdentity;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};
use tokio::time::{timeout_at, Instant};

use crate::connection::{rejection_code, ConnError, RouteHandle, RouteOpenRetry, SubcConnection};

type RouteKey = (BindIdentity, String);
type RouteSlot = Arc<AsyncMutex<Option<RouteHandle>>>;

pub(crate) struct RoleRoute {
    conn: Arc<SubcConnection>,
    role: &'static str,
    version: &'static str,
    check_describe: fn(&Value) -> Result<(), String>,
    routes: Mutex<HashMap<RouteKey, RouteSlot>>,
}

impl RoleRoute {
    pub(crate) fn new(
        conn: Arc<SubcConnection>,
        role: &'static str,
        version: &'static str,
        check_describe: fn(&Value) -> Result<(), String>,
    ) -> Self {
        Self {
            conn,
            role,
            version,
            check_describe,
            routes: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn close_session_routes(&self, bind: &BindIdentity) {
        let slots: Vec<_> = {
            let mut routes = self.routes.lock().unwrap();
            let keys: Vec<_> = routes
                .keys()
                .filter(|(key, _)| key == bind)
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
        request: &P,
        deadline: Instant,
        retry_transient: bool,
    ) -> Result<A, FetchError> {
        let body =
            serde_json::to_vec(request).map_err(|error| FetchError::Lost(error.to_string()))?;
        let slot = self
            .routes
            .lock()
            .unwrap()
            .entry((bind.clone(), provider.to_owned()))
            .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
            .clone();
        timeout_at(deadline, async {
            let mut guard = slot.lock().await;
            let mut retried = false;
            loop {
                if Instant::now() >= deadline {
                    return Err(expired());
                }
                let result = self
                    .attempt(provider, bind, &body, deadline, &mut guard)
                    .await;
                if retry_transient
                    && !retried
                    && matches!(
                        &result, Err(FetchError::Refused { code, .. }) if code == "transient"
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
            let (opened, describe) = self.open_and_describe(provider, bind, deadline).await?;
            lease = opened;
            let describe = describe.map_err(refused)?;
            (self.check_describe)(&describe).map_err(|problems| FetchError::Refused {
                code: broca_fetch_plan::admit::INVALID_ANSWER.into(),
                detail: Some(serde_json::json!({"provider": provider, "problems": problems})),
                retryable: false,
            })?;
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
        deadline: Instant,
    ) -> Result<Result<Value, subc_protocol::ErrorBody>, FetchError> {
        timeout_at(deadline, async {
            let (mut lease, describe) = self.open_and_describe(provider, bind, deadline).await?;
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
        deadline: Instant,
    ) -> Result<(RouteLease, Result<Value, subc_protocol::ErrorBody>), FetchError> {
        let route = self
            .conn
            .route_open_role_within(
                provider,
                bind,
                self.role,
                self.version,
                RouteOpenRetry {
                    deadline,
                    max_interval: Duration::from_secs(1),
                },
            )
            .await
            .map_err(open_error)?;
        let lease = RouteLease {
            conn: self.conn.clone(),
            route: Some(route),
        };
        let describe = match self
            .exchange(
                &route,
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
    serde_json::from_slice(reply).map_err(|_| FetchError::Refused {
        code: broca_fetch_plan::admit::INVALID_ANSWER.into(),
        detail: None,
        retryable: false,
    })
}

/// A role call's refusal; only `transient` is retried.
fn refused(refusal: subc_protocol::ErrorBody) -> FetchError {
    FetchError::Refused {
        retryable: refusal.code == "transient",
        code: refusal.code,
        detail: refusal.detail,
    }
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
fn open_error(error: ConnError) -> FetchError {
    match error {
        ConnError::Rejected(message) => {
            let code = rejection_code(&message);
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
