//! Fetch-plan admission's calls to providers over subc.
//!
//! Catalogs use scoped `tool-provider: v1` routes and tool envelopes. Declarations use
//! scoped `step-transform-provider: v2` role routes and method/params envelopes, just like
//! runtime hooks. A plan's compaction provider is asked only `role.describe`, on an unscoped
//! `compaction-provider: v1` role route like the one Setup later uses. All admission routes
//! close afterwards; no answer is shared across sessions.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_fetch_plan::{FetchError, Fetcher, CATALOG_OP};
use broca_protocol::{BindIdentity, SessionScope};
use cortexkit_role_step_transform_provider::subscription::DeclareRequest;
use cortexkit_role_tool_provider::catalog::CatalogRequest;
use serde_json::Value;
use subc_protocol::scope::{ScopeSelector, CAP_SCOPES_V1};
use subc_protocol::tool_call::ToolCallRequest;

use crate::connection::{RouteHandle, SubcConnection};
use crate::role_route::open_error;

/// The refusal code a provider answers a fetch with when the same fetch may succeed if sent
/// again; admission retries such a refusal until its deadline.
const TRANSIENT: &str = "transient";

/// How long one fetch waits for silence to end on its route. Admission's own deadline is the
/// real bound; this only keeps a single call from outliving it.
const FETCH_REPLY_TIMEOUT: Duration = broca_fetch_plan::ADMISSION_DEADLINE;

type CatalogSnapshot = futures::future::Shared<
    futures::future::BoxFuture<'static, Result<Arc<Vec<subc_control::CatalogEntry>>, FetchError>>,
>;

/// Fetches one session's plan over a subc consumer connection.
pub struct SubcPlanFetcher {
    conn: Arc<SubcConnection>,
    identity: BindIdentity,
    scope: Option<SessionScope>,
    direct_carrier_scope: Option<ScopeSelector>,
    opened: Mutex<Vec<RouteHandle>>,
    hooks: crate::SubcHookPlane,
    compaction: crate::SubcCompactionPlane,
    deadline: tokio::time::Instant,
    catalog_snapshot: Mutex<Option<CatalogSnapshot>>,
}

impl SubcPlanFetcher {
    /// A fetcher for the session `identity`, under its frozen `scope` when it has one.
    pub fn new(
        conn: Arc<SubcConnection>,
        identity: BindIdentity,
        scope: Option<SessionScope>,
    ) -> Self {
        Self {
            hooks: crate::SubcHookPlane::new(conn.clone()),
            compaction: crate::SubcCompactionPlane::new(conn.clone()),
            deadline: tokio::time::Instant::now() + broca_fetch_plan::ADMISSION_DEADLINE,
            conn,
            identity,
            scope,
            direct_carrier_scope: None,
            opened: Mutex::new(Vec::new()),
            catalog_snapshot: Mutex::new(None),
        }
    }

    /// Fetch tool catalogs under a caller-supplied scope as a Direct carrier, without
    /// creating a session or changing the session's frozen scope.
    pub fn with_direct_carrier_scope(mut self, scope: ScopeSelector) -> Self {
        self.direct_carrier_scope = Some(scope);
        self
    }

    /// Close every route this fetcher opened. Best effort: a route that cannot be closed is
    /// freed when the connection closes.
    pub async fn close(&self) {
        self.hooks.close_session_routes(&self.identity).await;
        let opened = std::mem::take(&mut *self.opened.lock().unwrap());
        for route in opened {
            let _ = self.conn.route_close(&route).await;
        }
    }

    async fn catalog_snapshot(&self) -> Result<Arc<Vec<subc_control::CatalogEntry>>, FetchError> {
        use futures::FutureExt;
        let snapshot = self
            .catalog_snapshot
            .lock()
            .unwrap()
            .get_or_insert_with(|| {
                let conn = self.conn.clone();
                // The shared future stays in the admission-local cache if an optional caller
                // stops waiting. Another provider must not start a duplicate control request.
                async move {
                    conn.catalog_list()
                        .await
                        .map(Arc::new)
                        .map_err(|error| FetchError::Lost(error.to_string()))
                }
                .boxed()
                .shared()
            })
            .clone();
        snapshot.await
    }

    /// Prefer the carrier's selector for catalog composition, otherwise use the session's
    /// frozen scope. Refuse a daemon without scoped routes rather than silently fetching
    /// an unscoped catalog.
    async fn selector(&self) -> Result<Option<ScopeSelector>, FetchError> {
        if let Some(selector) = &self.direct_carrier_scope {
            if self.scope.is_some() {
                return Err(FetchError::Refused {
                    code: "scope_unsupported".into(),
                    detail: Some(Value::String(
                        "a private Direct carrier selector cannot replace a frozen session scope"
                            .into(),
                    )),
                    retryable: false,
                });
            }
            crate::carrier_scope::verify_direct_carrier_scope(&self.conn, selector)
                .await
                .map_err(|error| FetchError::Refused {
                    code: error.split(':').next().unwrap_or(error).into(),
                    detail: Some(Value::String(error.into())),
                    retryable: false,
                })?;
            return Ok(Some(selector.clone()));
        }
        let Some(scope) = &self.scope else {
            return Ok(None);
        };
        match self.conn.server_capabilities().await {
            Ok(capabilities) if capabilities.iter().any(|c| c == CAP_SCOPES_V1) => {}
            Ok(_) => {
                return Err(FetchError::Refused {
                    code: "scope_unsupported".into(),
                    detail: None,
                    retryable: false,
                })
            }
            Err(error) => return Err(FetchError::Lost(error.to_string())),
        }
        let selector = crate::subc_plane::frozen_scope_selector(scope).map_err(|detail| {
            FetchError::Refused {
                code: "scope_unverifiable".into(),
                detail: Some(Value::String(detail)),
                retryable: false,
            }
        })?;
        Ok(Some(selector))
    }

    async fn call(&self, provider: &str, op: &str, arguments: Value) -> Result<Value, FetchError> {
        let selector = self.selector().await?;
        let route = self
            .conn
            .route_open_tool_provider_v1(provider, &self.identity, selector)
            .await
            .map_err(|error| {
                open_error(if self.direct_carrier_scope.is_some() {
                    crate::carrier_scope::private_route_error(error)
                } else {
                    error
                })
            })?;
        self.opened.lock().unwrap().push(route);
        let body = fetch_body(op, arguments)?;
        match self
            .conn
            .route_unary_reply(&route, body, FETCH_REPLY_TIMEOUT)
            .await
        {
            Ok(Ok(reply)) => decode_reply(provider, &reply),
            Ok(Err(refusal)) => {
                let retryable = refusal.code == TRANSIENT;
                Err(FetchError::provider_refused(
                    refusal.code,
                    refusal.detail,
                    &refusal.message,
                    retryable,
                ))
            }
            Err(error) => Err(FetchError::Lost(error.to_string())),
        }
    }
}

fn fetch_body(op: &str, arguments: Value) -> Result<Vec<u8>, FetchError> {
    let preset = arguments
        .get("preset")
        .or_else(|| {
            arguments
                .get("system_text")
                .and_then(|item| item.get("preset"))
        })
        .and_then(Value::as_str)
        .map(str::to_owned);
    serde_json::to_vec(&ToolCallRequest {
        name: op.to_owned(),
        arguments,
        tool_call_id: None,
        progress_token: None,
        call_key: None,
        schema_pin: None,
        origin: None,
        preset,
        output_refs: None,
    })
    .map_err(|error| FetchError::Lost(error.to_string()))
}

/// A catalog is a bare CatalogAnswer, not a tool-call result envelope.
fn decode_reply(provider: &str, reply: &[u8]) -> Result<Value, FetchError> {
    let value: Value = serde_json::from_slice(reply).map_err(|error| {
        invalid_answer(&format!(
            "reply is not JSON ({error}): {}",
            String::from_utf8_lossy(reply)
        ))
    })?;
    let wrapped = value
        .get("content")
        .and_then(Value::as_array)
        .filter(|_| value.get("catalog_digest").is_none() && value.get("subscriptions").is_none());
    match wrapped {
        Some(content) => {
            // Keep wrapped catalogs for one release so WARNs identify providers needing a fix.
            // Delete this fallback in the next release if no wrapped-catalog WARN appears.
            tracing::warn!(target: "subc.plan_fetch", provider_module = provider,
                "provider returned a wrapped tool.catalog answer; expected a bare CatalogAnswer");
            let text = content
                .iter()
                .find_map(|part| part.get("text").and_then(Value::as_str));
            if value.get("isError").and_then(Value::as_bool) == Some(true) {
                return Err(invalid_answer(&format!(
                    "provider answered with an error result: {}",
                    text.unwrap_or("<no text part>")
                )));
            }
            let text = text.ok_or_else(|| invalid_answer("wrapped reply has no text part"))?;
            serde_json::from_str(text).map_err(|error| {
                invalid_answer(&format!("wrapped reply text is not JSON ({error}): {text}"))
            })
        }
        None => Ok(value),
    }
}

/// A reply broca could not read as an answer. The message says what was wrong and quotes the
/// start of what the provider sent (capped by `provider_refused`), so the admission warning
/// names the cause; before, every such case logged only `invalid_answer`.
fn invalid_answer(message: &str) -> FetchError {
    FetchError::provider_refused(
        broca_fetch_plan::admit::INVALID_ANSWER.into(),
        None,
        message,
        false,
    )
}

/// Admits plans over one subc consumer connection, with a fresh [`SubcPlanFetcher`] (and fresh
/// routes) for every admission.
pub struct SubcPlanAdmitter {
    conn: Arc<SubcConnection>,
}

impl SubcPlanAdmitter {
    pub fn new(conn: Arc<SubcConnection>) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl broca_fetch_plan::PlanAdmitter for SubcPlanAdmitter {
    fn admits_output_refs(&self) -> bool {
        true
    }

    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        session: &BindIdentity,
        scope: Option<&SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        let fetcher = SubcPlanFetcher::new(self.conn.clone(), session.clone(), scope.cloned());
        let admitted = broca_fetch_plan::admit(
            plan,
            &fetcher,
            session,
            broca_fetch_plan::ADMISSION_DEADLINE,
        )
        .await;
        fetcher.close().await;
        admitted
    }
}

#[async_trait]
impl Fetcher for SubcPlanFetcher {
    async fn call_keepalive_supported(&self, provider: &str) -> Result<bool, FetchError> {
        let entries = self.catalog_snapshot().await?;
        Ok(entries
            .iter()
            .find(|entry| entry.module_id == provider)
            .and_then(|entry| entry.capabilities.as_ref())
            .is_some_and(|caps| {
                caps.provides.iter().any(|capability| {
                    capability == cortexkit_role_tool_provider::CALL_KEEPALIVE_CAPABILITY
                })
            }))
    }
    async fn system_text_default_supported(&self, provider: &str) -> Result<bool, FetchError> {
        let entries = match self.catalog_snapshot().await {
            Ok(entries) => entries,
            Err(error) => {
                // Required preset discovery keeps its existing retry policy. Only a
                // successful snapshot, or an optional lookup, is retained for admission.
                self.catalog_snapshot.lock().unwrap().take();
                return Err(error);
            }
        };
        Ok(entries
            .iter()
            .find(|entry| entry.module_id == provider)
            .and_then(|entry| entry.capabilities.as_ref())
            .is_some_and(|caps| {
                caps.provides.iter().any(|capability| {
                    capability == cortexkit_role_tool_provider::SYSTEM_TEXT_DEFAULT
                })
            }))
    }
    async fn catalog(&self, provider: &str, request: &CatalogRequest) -> Result<Value, FetchError> {
        let arguments =
            serde_json::to_value(request).map_err(|error| FetchError::Lost(error.to_string()))?;
        self.call(provider, CATALOG_OP, arguments).await
    }

    async fn declare(&self, provider: &str, request: &DeclareRequest) -> Result<Value, FetchError> {
        self.hooks
            .declare(
                provider,
                &self.identity,
                self.scope.as_ref(),
                request,
                self.deadline,
            )
            .await
    }

    async fn describe_compaction(
        &self,
        provider: &str,
    ) -> Result<broca_fetch_plan::DescribeAnswer, FetchError> {
        self.compaction
            .describe(provider, &self.identity, self.scope.as_ref(), self.deadline)
            .await
    }
}

#[cfg(test)]
mod carrier_catalog_tests {
    use super::*;
    use serde_json::json;

    fn capabilities(capabilities: &[&str]) -> Vec<u8> {
        serde_json::to_vec(&subc_control::ClientControlResponse::ServerDescribe {
            protocol_ver: 1,
            subc_ops: vec![],
            capabilities: capabilities.iter().map(|cap| (*cap).into()).collect(),
            connected_clients: 1,
            counters: None,
            build_git_sha: None,
            build_lock_digest: None,
            capability_requirements: vec![],
            machine_id: None,
            config_path: None,
        })
        .unwrap()
    }

    fn private_selector() -> ScopeSelector {
        "rig/run/1"
            .parse::<crate::carrier_scope::CarrierScope>()
            .unwrap()
            .read_proof(&mut "ab".repeat(32).as_bytes())
            .unwrap()
    }

    #[tokio::test]
    async fn direct_carrier_catalog_scope_reaches_every_provider() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let selector = private_selector();
        let fetcher = SubcPlanFetcher::new(
            Arc::new(conn),
            BindIdentity::new("/project", "runner", "catalogs"),
            None,
        )
        .with_direct_carrier_scope(selector.clone());
        let fetch = async {
            for provider in ["aft", "other"] {
                assert_eq!(
                    fetcher.call(provider, CATALOG_OP, json!({})).await.unwrap(),
                    json!({"tools": []})
                );
            }
        };
        let serve = async {
            let describe = daemon.next_request().await;
            assert_eq!(
                serde_json::from_slice::<Value>(&describe.body).unwrap()["op"],
                "server.describe"
            );
            assert!(daemon.reply(
                &describe,
                capabilities(&[
                    CAP_SCOPES_V1,
                    subc_protocol::manifest::CAP_SCOPE_BIND_PROOF_V1,
                ])
            ));
            for provider in ["aft", "other"] {
                let open = daemon.next_request().await;
                let body: Value = serde_json::from_slice(&open.body).unwrap();
                assert_eq!(body["op"], "route.open");
                assert_eq!(body["target"]["module_id"], provider);
                assert_eq!(body["scope"], serde_json::to_value(&selector).unwrap());
                assert!(body.get("consumer_identity").is_none());
                assert!(daemon.reply(
                    &open,
                    serde_json::to_vec(&subc_control::ClientControlResponse::RouteOpen {
                        route_channel: 1,
                        route_epoch: 1,
                    },)
                    .unwrap()
                ));
                let catalog = daemon.next_request().await;
                assert_eq!(catalog.header.channel, 1);
                assert_eq!(catalog.body, br#"{"name":"tool.catalog","arguments":{}}"#);
                assert!(daemon.reply(&catalog, br#"{"tools":[]}"#.to_vec()));
            }
        };
        tokio::join!(fetch, serve);
    }

    #[tokio::test]
    async fn direct_carrier_catalog_requires_proof_capability_without_opening_a_route() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let fetcher = SubcPlanFetcher::new(
            Arc::new(conn),
            BindIdentity::new("/project", "runner", "catalogs"),
            None,
        )
        .with_direct_carrier_scope(private_selector());
        let (result, ()) = tokio::join!(fetcher.selector(), async {
            let describe = daemon.next_request().await;
            assert!(daemon.reply(&describe, capabilities(&[CAP_SCOPES_V1])));
        });
        assert!(
            matches!(result, Err(FetchError::Refused { code, retryable: false, .. }) if code == "scope_unsupported")
        );
        assert!(daemon.take_written_for_test().is_empty());
    }

    #[tokio::test]
    async fn unscoped_composition_pins_catalog_requests_and_output_bytes() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let fetcher = SubcPlanFetcher::new(
            Arc::new(conn),
            BindIdentity::new("/project", "runner", "unscoped-plan"),
            None,
        );
        let items = json!({"tool_items": [{"provider": "aft", "preset": "reader", "params": {"scope": "read"}}],
            "system_text_items": [], "step_transform_items": []});
        let (plan, ()) = tokio::join!(
            broca_fetch_plan::compose(&items, &fetcher, broca_fetch_plan::ADMISSION_DEADLINE),
            async {
                let open = daemon.next_request().await;
                assert_eq!(open.body, br#"{"op":"route.open","target":{"kind":"tool_provider","module_id":"aft"},"identity":{"harness":"runner","session":"unscoped-plan"},"role_versions":{"tool-provider":"v1"}}"#);
                assert!(daemon.reply(
                    &open,
                    serde_json::to_vec(&subc_control::ClientControlResponse::RouteOpen {
                        route_channel: 1,
                        route_epoch: 1,
                    },)
                    .unwrap()
                ));
                let catalog = daemon.next_request().await;
                assert_eq!(catalog.body, br#"{"name":"tool.catalog","arguments":{"params":{"scope":"read"},"preset":"reader"},"preset":"reader"}"#);
                assert!(daemon.reply(&catalog, serde_json::to_vec(&json!({
                    "generation": "live", "catalog_digest": "opaque", "tools": [
                        {"name": "zoom", "description": "Zoom", "input_schema": {}, "schema_digest": "z",
                         "semantics": 1, "capabilities": ["z/v1", "a/v1"]},
                        {"name": "read", "description": "Read", "input_schema": {}, "schema_digest": "r",
                         "semantics": 1, "capabilities": ["code.read/v1"]}
                    ]
                })).unwrap()));
            }
        );
        assert_eq!(
            format!("{}\n", plan.unwrap()),
            concat!(
                r#"{"composition":{"providers":[{"provider":"aft","tools":[{"capabilities":["code.read/v1"],"name":"read"},{"capabilities":["a/v1","z/v1"],"name":"zoom"}]}],"step_transform_items":[]},"composition_digest":"9881838597e87f35274e5d544969fb8f626aeee745ea81057a2f03909bf25fd8","step_transform_items":[],"system_text_items":[],"tool_items":[{"params":{"scope":"read"},"preset":"reader","provider":"aft"}]}"#,
                "\n"
            )
        );
        assert!(daemon.take_written_for_test().is_empty());
    }
}

#[cfg(test)]
mod preset_tests {
    use super::*;

    #[tokio::test]
    async fn production_admitter_enables_output_reference_plans() {
        use broca_fetch_plan::PlanAdmitter;
        let (connection, _, _) = SubcConnection::in_process_for_test(1, 1);
        assert!(SubcPlanAdmitter::new(Arc::new(connection)).admits_output_refs());
    }

    #[tokio::test]
    async fn fetch_and_consent_without_scopes_fail_closed_on_the_same_cached_capabilities() {
        let (conn, _, mut module) = SubcConnection::in_process_for_test(1, 1);
        let conn = Arc::new(conn);
        let identity = BindIdentity::new("/p", "broca", "s");
        let scope = SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: "broca".into(),
            },
            scope_ref: "s".into(),
            scope_epoch: 7,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        };
        let fetcher = SubcPlanFetcher::new(conn.clone(), identity.clone(), Some(scope.clone()));
        let (selector, ()) = tokio::join!(fetcher.selector(), async {
            let request = module.next_request().await;
            let reply = subc_control::ClientControlResponse::ServerDescribe {
                protocol_ver: 1,
                subc_ops: vec![],
                capabilities: vec![],
                connected_clients: 1,
                counters: None,
                build_git_sha: None,
                build_lock_digest: None,
                capability_requirements: vec![],
                machine_id: None,
                config_path: None,
            };
            assert!(module.reply(&request, serde_json::to_vec(&reply).unwrap()));
        });
        assert!(
            matches!(selector, Err(FetchError::Refused { code, retryable: false, .. }) if code == "scope_unsupported")
        );
        let consent = crate::ConsentClient::open(
            conn,
            &identity,
            &scope,
            crate::RouteOpenRetry::default_patience(),
        )
        .await;
        assert!(
            matches!(consent, Err(crate::ConsentError::Refused { code, .. }) if code == "scope_unsupported")
        );
        assert!(
            module.take_written_for_test().is_empty(),
            "neither scoped operation may open an unscoped route"
        );
    }

    /// A reply broca cannot read carries what was wrong, so the admission warning names the cause.
    #[test]
    fn unreadable_replies_carry_their_cause() {
        let cases = [
            (
                br#"{"content":[{"type":"text","text":"preset worker requires a scope"}],"isError":true}"#.as_slice(),
                "provider answered with an error result: preset worker requires a scope",
            ),
            (b"not json".as_slice(), "reply is not JSON"),
            (
                br#"{"content":[{"type":"text","text":"plain words"}]}"#.as_slice(),
                "wrapped reply text is not JSON",
            ),
        ];
        for (reply, expected) in cases {
            match decode_reply("preset-provider", reply) {
                Err(FetchError::ProviderRefused {
                    code,
                    message,
                    retryable,
                    ..
                }) => {
                    assert_eq!(code, broca_fetch_plan::admit::INVALID_ANSWER);
                    assert!(!retryable);
                    assert!(message.contains(expected), "{message}");
                }
                other => panic!("expected a refusal naming its cause, got {other:?}"),
            }
        }
    }

    #[test]
    fn wrapped_catalog_warns_once_with_provider_while_bare_catalog_is_quiet() {
        let answer = serde_json::json!({"catalog_digest": "digest", "tools": []});
        let wrapped =
            serde_json::json!({"content": [{"type": "text", "text": answer.to_string()}]});
        let capture = broca_test_support::LogCapture::new();
        capture.in_scope(|| {
            assert_eq!(
                decode_reply("bare-provider", &serde_json::to_vec(&answer).unwrap()).unwrap(),
                answer
            );
            assert_eq!(
                decode_reply("wrapped-provider", &serde_json::to_vec(&wrapped).unwrap()).unwrap(),
                answer
            );
        });
        let warnings: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| {
                event.target == "subc.plan_fetch" && event.level == tracing::Level::WARN
            })
            .collect();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert_eq!(warnings[0].fields["provider_module"], "wrapped-provider");
    }

    #[test]
    fn catalog_envelope_preserves_item_preset_and_composition_bytes() {
        let composition = serde_json::json!({"providers": [{"provider": "aft", "tools": []}], "step_transform_items": []});
        for arguments in [
            serde_json::json!({"preset": "worker", "params": {}, "composition": composition}),
            serde_json::json!({"params": {}, "system_text": {"preset": "reader", "params": {}}, "composition": composition}),
        ] {
            let expected = arguments
                .get("preset")
                .unwrap_or(&arguments["system_text"]["preset"]);
            let body = fetch_body(CATALOG_OP, arguments.clone()).unwrap();
            let request: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(&request["preset"], expected);
            assert_eq!(request["arguments"], arguments);
            assert_eq!(
                serde_json::to_vec(&request["arguments"]["composition"]).unwrap(),
                serde_json::to_vec(&composition).unwrap()
            );
        }
    }
}

#[cfg(test)]
mod default_text_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn a_cancelled_optional_waiter_does_not_restart_the_shared_catalog_request() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let fetcher = SubcPlanFetcher::new(
            Arc::new(conn),
            BindIdentity::new("/project", "runner", "cancelled-probe"),
            None,
        );
        let request = {
            let first = fetcher.call_keepalive_supported("provider");
            tokio::pin!(first);
            assert!(futures::poll!(&mut first).is_pending());
            daemon.next_request().await
        };
        let mut entry =
            serde_json::to_value(subc_control::CatalogEntry::new("provider", vec![], vec![]))
                .unwrap();
        entry["capabilities"] =
            json!({"provides":[cortexkit_role_tool_provider::CALL_KEEPALIVE_CAPABILITY]});
        let response = subc_control::ClientControlResponse::CatalogList {
            generation: 1,
            modules: vec![serde_json::from_value(entry).unwrap()],
            subc_ops: vec![],
        };
        assert!(daemon.reply(&request, serde_json::to_vec(&response).unwrap()));
        assert!(fetcher.call_keepalive_supported("provider").await.unwrap());
        assert!(daemon.take_written_for_test().is_empty());
    }

    #[tokio::test]
    async fn required_system_text_discovery_can_retry_a_failed_snapshot() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let fetcher = SubcPlanFetcher::new(
            Arc::new(conn),
            BindIdentity::new("/project", "runner", "required-retry"),
            None,
        );
        let (first, request) =
            tokio::join!(fetcher.system_text_default_supported("provider"), async {
                let request = daemon.next_request().await;
                assert!(daemon.refuse(&request, "module_timeout"));
                request
            });
        assert!(first.is_err());
        let (second, ()) = tokio::join!(fetcher.system_text_default_supported("provider"), async {
            let retry = daemon.next_request().await;
            assert_ne!(retry.header.corr, request.header.corr);
            let mut entry =
                serde_json::to_value(subc_control::CatalogEntry::new("provider", vec![], vec![]))
                    .unwrap();
            entry["capabilities"] =
                json!({"provides":[cortexkit_role_tool_provider::SYSTEM_TEXT_DEFAULT]});
            let response = subc_control::ClientControlResponse::CatalogList {
                generation: 1,
                modules: vec![serde_json::from_value(entry).unwrap()],
                subc_ops: vec![],
            };
            assert!(daemon.reply(&retry, serde_json::to_vec(&response).unwrap()));
        });
        assert!(second.unwrap());
        assert!(!fetcher.call_keepalive_supported("provider").await.unwrap());
        assert!(daemon.take_written_for_test().is_empty());
    }

    #[tokio::test]
    async fn keepalive_discovery_shares_one_snapshot_with_existing_system_text_discovery() {
        let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
        let fetcher = SubcPlanFetcher::new(
            Arc::new(conn),
            BindIdentity::new("/project", "runner", "shared-catalog"),
            None,
        );
        let read = async {
            assert!(fetcher
                .system_text_default_supported("provider")
                .await
                .unwrap());
            let (first, second) = tokio::join!(
                fetcher.call_keepalive_supported("provider"),
                fetcher.call_keepalive_supported("other"),
            );
            assert!(first.unwrap());
            assert!(!second.unwrap());
        };
        let reply = async {
            let request = daemon.next_request().await;
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["op"], "catalog.list");
            let mut entry =
                serde_json::to_value(subc_control::CatalogEntry::new("provider", vec![], vec![]))
                    .unwrap();
            entry["capabilities"] = json!({"provides":[
                cortexkit_role_tool_provider::SYSTEM_TEXT_DEFAULT,
                cortexkit_role_tool_provider::CALL_KEEPALIVE_CAPABILITY,
            ]});
            let response = subc_control::ClientControlResponse::CatalogList {
                generation: 1,
                modules: vec![serde_json::from_value(entry).unwrap()],
                subc_ops: vec![],
            };
            assert!(daemon.reply(&request, serde_json::to_vec(&response).unwrap()));
        };
        tokio::join!(read, reply);
        assert!(
            daemon.take_written_for_test().is_empty(),
            "no extra daemon request after the existing lookup"
        );
    }

    #[tokio::test]
    async fn keepalive_promise_comes_only_from_the_matching_provider_manifest() {
        for (module, provides, expected) in [
            (
                "provider",
                vec![cortexkit_role_tool_provider::CALL_KEEPALIVE_CAPABILITY],
                true,
            ),
            ("provider", vec![], false),
            (
                "other",
                vec![cortexkit_role_tool_provider::CALL_KEEPALIVE_CAPABILITY],
                false,
            ),
        ] {
            let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
            let fetcher = SubcPlanFetcher::new(
                Arc::new(conn),
                BindIdentity::new("/project", "runner", "catalog"),
                None,
            );
            let read = fetcher.call_keepalive_supported("provider");
            let reply = async {
                let request = daemon.next_request().await;
                let body: Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(body["op"], "catalog.list");
                let mut entry =
                    serde_json::to_value(subc_control::CatalogEntry::new(module, vec![], vec![]))
                        .unwrap();
                entry["capabilities"] = json!({"provides":provides});
                let response = subc_control::ClientControlResponse::CatalogList {
                    generation: 1,
                    modules: vec![serde_json::from_value(entry).unwrap()],
                    subc_ops: vec![],
                };
                assert!(daemon.reply(&request, serde_json::to_vec(&response).unwrap()));
            };
            let (supported, ()) = tokio::join!(read, reply);
            assert_eq!(supported.unwrap(), expected);
            assert!(daemon.take_written_for_test().is_empty());
        }
    }

    #[tokio::test]
    async fn system_text_default_uses_matching_daemon_catalog_declaration() {
        for (module, provides, expected) in [
            (
                "persona",
                vec![cortexkit_role_tool_provider::SYSTEM_TEXT_DEFAULT],
                true,
            ),
            ("persona", vec![], false),
            (
                "another-provider",
                vec![cortexkit_role_tool_provider::SYSTEM_TEXT_DEFAULT],
                false,
            ),
        ] {
            let (conn, _, mut daemon) = SubcConnection::in_process_for_test(1, 1);
            let fetcher = SubcPlanFetcher::new(
                Arc::new(conn),
                BindIdentity::new("/synthetic/default-text", "runner", "catalog-guard"),
                None,
            );
            let read = fetcher.system_text_default_supported("persona");
            let reply = async {
                let request = daemon.next_request().await;
                let body: Value = serde_json::from_slice(&request.body).unwrap();
                assert_eq!(body["op"], "catalog.list");
                let mut entry =
                    serde_json::to_value(subc_control::CatalogEntry::new(module, vec![], vec![]))
                        .unwrap();
                entry["capabilities"] = json!({"provides":provides});
                let entry = serde_json::from_value(entry).unwrap();
                let response = subc_control::ClientControlResponse::CatalogList {
                    generation: 1,
                    modules: vec![entry],
                    subc_ops: vec![],
                };
                assert!(daemon.reply(&request, serde_json::to_vec(&response).unwrap()));
            };
            let (supported, ()) = tokio::join!(read, reply);
            assert_eq!(supported.unwrap(), expected);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), daemon.next_request())
                    .await
                    .is_err(),
                "checking a capability must not forward a provider request"
            );
        }
    }
}
