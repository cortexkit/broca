//! The module-level `step-transform-provider/v2` route, shared by hooks and declarations.
//! Routes carry the session's frozen scope; operations have no tool-call envelope.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{HookPlane, HookUnavailable, STEP_TRANSFORM_HOOK_CAP};
use broca_fetch_plan::FetchError;
use broca_protocol::{BindIdentity, SessionScope};
use cortexkit_role_step_transform_provider::{
    answer::HookAnswer, describe::check_describe, hook::HookCall, ops,
    subscription::DeclareRequest, OpRequest,
};
use serde_json::Value;
use tokio::time::Instant;

use crate::{connection::SubcConnection, role_route::RoleRoute};

/// Lazily opens one route per caller session, provider and frozen scope. Calls sharing a route serialize;
/// waiting for that route, discovery, route-open retries and the RPC all consume the caller's
/// deadline. Different sessions and providers have independent route slots.
pub struct SubcHookPlane {
    route: RoleRoute,
}

impl SubcHookPlane {
    pub fn new(conn: Arc<SubcConnection>) -> Self {
        Self {
            route: RoleRoute::new(
                conn,
                cortexkit_role_step_transform_provider::ROLE,
                cortexkit_role_step_transform_provider::VERSION,
                |describe| {
                    check_describe(describe)
                        .map(|_| ())
                        .map_err(|problems| format!("{problems:?}"))
                },
            ),
        }
    }

    /// Admission entry point. Returns the declaration JSON without reserializing or pruning
    /// its members, so admission can validate and freeze it. The deadline is admission's
    /// shared absolute deadline, not a new window for each route-open attempt.
    pub async fn declare(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        request: &DeclareRequest,
        deadline: Instant,
    ) -> Result<Value, FetchError> {
        self.route
            .call(
                provider,
                bind,
                scope,
                &OpRequest::new(ops::TRANSFORM_DECLARE, request),
                deadline,
                false,
            )
            .await
    }

    /// Call with an explicit end-to-end deadline, for callers that already know the frozen
    /// subscription budget. Core's trait caller also wraps its entire future in that budget.
    pub async fn hook_with_deadline(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        call: HookCall,
        deadline: Instant,
    ) -> Result<HookAnswer, HookUnavailable> {
        let deadline =
            deadline.min(Instant::now() + Duration::from_millis(STEP_TRANSFORM_HOOK_CAP));
        self.route
            .call(
                provider,
                bind,
                scope,
                &OpRequest::new(ops::TRANSFORM_HOOK, &call),
                deadline,
                true,
            )
            .await
            .map_err(|_| HookUnavailable)
    }

    /// Release this session's routes when its actor is retired. Best effort, like tool routes.
    pub async fn close_session_routes(&self, bind: &BindIdentity) {
        self.route.close_session_routes(bind).await;
    }
}

#[async_trait]
impl HookPlane for SubcHookPlane {
    async fn hook(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&SessionScope>,
        call: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        // HookPlane has no budget parameter. Core bounds this future by the subscription's
        // smaller budget; this ceiling also bounds a direct trait call without that wrapper.
        self.hook_with_deadline(
            provider,
            bind,
            scope,
            call,
            Instant::now() + Duration::from_millis(STEP_TRANSFORM_HOOK_CAP),
        )
        .await
    }
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn pre_tool_hook_reopens_after_a_daemon_upgrade_refusal() {
    use crate::daemon_upgrade;
    use serde_json::json;

    // The upgrade notice is process-wide and would affect other tests' retry decisions.
    if !daemon_upgrade::run_in_test_child(
        module_path!(),
        "pre_tool_hook_reopens_after_a_daemon_upgrade_refusal",
    ) {
        return;
    }
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(7, 1);
    let conn = Arc::new(conn);
    let hook = SubcHookPlane {
        route: RoleRoute::without_discovery(conn.clone(), "step-transform-provider", "2"),
    };
    let bind = BindIdentity::new("/project", "broca", "pre-tool");
    let call = serde_json::from_value(
        json!({"hook":"pre_tool", "step_id":"step-1", "phase":"mutate",
        "tool":"read", "tool_call_id":"call-1", "input":{}, "params":{}}),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_millis(500);
    let run = hook.hook_with_deadline("transform", &bind, None, call, deadline);
    tokio::pin!(run);
    let open = tokio::select! {
        request = daemon.next_request() => request,
        result = &mut run => panic!("hook ended before opening: {result:?}"),
    };
    assert_eq!(open.header.channel, 0);
    assert!(daemon.reply(
        &open,
        br#"{"op":"route.open","route_channel":7,"route_epoch":1}"#.to_vec()
    ));
    let first = tokio::select! {
        request = daemon.next_request() => request,
        result = &mut run => panic!("hook did not send its request: {result:?}"),
    };
    assert_eq!(first.header.channel, 7);
    let ceiling = Instant::now() + Duration::from_secs(1);
    daemon_upgrade::announce(ceiling);
    assert!(daemon.refuse_for_daemon_upgrade(&first));
    assert!(
        futures::poll!(&mut run).is_pending(),
        "a refused pre-tool hook must wait for resume"
    );
    assert!(
        daemon.try_frame().is_none(),
        "no reopen before the module is resumed"
    );
    daemon_upgrade::resumed(ceiling);
    let reopen = tokio::select! {
        request = daemon.next_request() => request,
        result = &mut run => panic!("hook reported an upgrade refusal as failure: {result:?}"),
    };
    assert_eq!(reopen.header.channel, 0);
    assert_eq!(
        reopen.body, open.body,
        "the replacement keeps the route declarations"
    );
    conn.allow_test_route(8, 2);
    assert!(daemon.reply(
        &reopen,
        br#"{"op":"route.open","route_channel":8,"route_epoch":2}"#.to_vec()
    ));
    let retry = tokio::select! {
        request = daemon.next_request() => request,
        result = &mut run => panic!("hook did not retry on the new route: {result:?}"),
    };
    assert_eq!(retry.header.channel, 8);
    assert_eq!(retry.body, first.body);
    assert!(daemon.reply(&retry, br#"{"answer":"pass"}"#.to_vec()));
    assert!(run.await.is_ok());
    daemon_upgrade::abandon();
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn a_scoped_pre_tool_hook_waits_to_discover_scopes_on_the_replacement() {
    use crate::daemon_upgrade;
    use serde_json::json;

    // The upgrade notice is process-wide and would affect other tests' scope discovery.
    if !daemon_upgrade::run_in_test_child(
        module_path!(),
        "a_scoped_pre_tool_hook_waits_to_discover_scopes_on_the_replacement",
    ) {
        return;
    }
    let (conn, _, _) = SubcConnection::in_process_for_test(7, 1);
    let (replacement, _, mut daemon) = SubcConnection::in_process_for_test(8, 2);
    conn.deactivate_for_test();
    let conn = Arc::new(conn);
    let hook = SubcHookPlane {
        route: RoleRoute::without_discovery(conn.clone(), "step-transform-provider", "2"),
    };
    let bind = BindIdentity::new("/project", "broca", "scoped-hook");
    let scope = SessionScope {
        owner: broca_protocol::BindPrincipal::Reserved {
            module_id: "owner".into(),
        },
        agent_id_frozen: false,
        scope_ref: "scope".into(),
        scope_epoch: 7,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
    };
    let call = serde_json::from_value(
        json!({"hook":"pre_tool", "step_id":"step-1", "phase":"mutate",
        "tool":"read", "tool_call_id":"call-1", "input":{}, "params":{}}),
    )
    .unwrap();
    let ceiling = Instant::now() + Duration::from_secs(1);
    daemon_upgrade::announce(ceiling);
    let run = hook.hook_with_deadline(
        "transform",
        &bind,
        Some(&scope),
        call,
        Instant::now() + Duration::from_millis(500),
    );
    tokio::pin!(run);
    assert!(
        futures::poll!(&mut run).is_pending(),
        "inactivity must not fail a scoped hook before its budget expires"
    );
    daemon_upgrade::resumed(ceiling);
    conn.replace_generation_for_test(&replacement);
    for (op, response) in [
        (
            "server.describe",
            json!({"op":"server.describe", "protocol_ver":1,"subc_ops":[],"capabilities":["scopes/v1"],"connected_clients":1}),
        ),
        (
            "route.open",
            json!({"op":"route.open","route_channel":8,"route_epoch":2}),
        ),
        ("transform.hook", json!({"answer":"pass"})),
    ] {
        let next = tokio::select! {
            request = daemon.next_request() => request,
            result = &mut run => panic!("hook failed before {op}: {result:?}"),
        };
        let body: serde_json::Value = serde_json::from_slice(&next.body).unwrap();
        assert_eq!(body.get("op").unwrap_or(&body["method"]), op);
        if op == "route.open" {
            assert_eq!(body["scope"]["ref"], "scope");
        }
        assert!(daemon.reply(&next, serde_json::to_vec(&response).unwrap()));
    }
    assert!(run.await.is_ok());
    daemon_upgrade::abandon();
}
