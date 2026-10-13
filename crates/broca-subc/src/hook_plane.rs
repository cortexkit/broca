//! The module-level `step-transform-provider/v1` route, shared by hooks and declarations.
//! No scope selector or tool-call envelope belongs on this role's surface.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{HookPlane, HookUnavailable, STEP_TRANSFORM_HOOK_CAP};
use broca_fetch_plan::FetchError;
use broca_protocol::BindIdentity;
use cortexkit_role_step_transform_provider::{
    answer::HookAnswer, describe::check_describe, hook::HookCall, ops,
    subscription::DeclareRequest, OpRequest,
};
use serde_json::Value;
use tokio::time::Instant;

use crate::{connection::SubcConnection, role_route::RoleRoute};

/// Lazily opens one route per caller session and provider. Calls sharing a route serialize;
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
        request: &DeclareRequest,
        deadline: Instant,
    ) -> Result<Value, FetchError> {
        self.route
            .call(
                provider,
                bind,
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
        call: HookCall,
        deadline: Instant,
    ) -> Result<HookAnswer, HookUnavailable> {
        // Identity in a request is the caller's durable key, never the bind's `runner` value.
        // Refuse a mismatched request rather than silently sending it to another conversation.
        if call.session != bind.session || call.harness != bind.harness {
            return Err(HookUnavailable);
        }
        let deadline =
            deadline.min(Instant::now() + Duration::from_millis(STEP_TRANSFORM_HOOK_CAP));
        self.route
            .call(
                provider,
                bind,
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
        call: HookCall,
    ) -> Result<HookAnswer, HookUnavailable> {
        // HookPlane has no budget parameter. Core bounds this future by the subscription's
        // smaller budget; this ceiling also bounds a direct trait call without that wrapper.
        self.hook_with_deadline(
            provider,
            bind,
            call,
            Instant::now() + Duration::from_millis(STEP_TRANSFORM_HOOK_CAP),
        )
        .await
    }
}
