//! Fetch-plan admission's calls to providers over subc.
//!
//! Catalogs use scoped `tool-provider: v1` routes and tool envelopes. Declarations use
//! unscoped `step-transform-provider: v1` role routes and method/params envelopes, just like
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

/// Fetches one session's plan over a subc consumer connection.
pub struct SubcPlanFetcher {
    conn: Arc<SubcConnection>,
    identity: BindIdentity,
    scope: Option<SessionScope>,
    opened: Mutex<Vec<RouteHandle>>,
    hooks: crate::SubcHookPlane,
    compaction: crate::SubcCompactionPlane,
    deadline: tokio::time::Instant,
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
            opened: Mutex::new(Vec::new()),
        }
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

    /// The scope selector the session's fetch routes open under, from its frozen scope (`None`
    /// for an unscoped session). Refused when the daemon does not support scoped routes, since
    /// it would ignore the selector and open an unscoped route.
    async fn selector(&self) -> Result<Option<ScopeSelector>, FetchError> {
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
            .map_err(open_error)?;
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
    async fn catalog(&self, provider: &str, request: &CatalogRequest) -> Result<Value, FetchError> {
        let arguments =
            serde_json::to_value(request).map_err(|error| FetchError::Lost(error.to_string()))?;
        self.call(provider, CATALOG_OP, arguments).await
    }

    async fn declare(&self, provider: &str, request: &DeclareRequest) -> Result<Value, FetchError> {
        self.hooks
            .declare(provider, &self.identity, request, self.deadline)
            .await
    }

    async fn describe_compaction(
        &self,
        provider: &str,
    ) -> Result<broca_fetch_plan::DescribeAnswer, FetchError> {
        self.compaction
            .describe(provider, &self.identity, self.deadline)
            .await
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
