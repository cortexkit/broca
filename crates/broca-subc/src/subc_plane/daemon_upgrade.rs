//! Reopen a tool route only when the daemon proves the failed request was not delivered.

use super::*;
use crate::daemon_upgrade::Retry;

pub(super) fn refused(outcome: &Result<serde_json::Value, ConnError>) -> bool {
    matches!(outcome, Err(ConnError::Rejected(error)) if crate::daemon_upgrade::is_switch_refusal(error))
}

pub(super) fn unverified_scope() -> DispatchResult {
    DispatchResult::NotSent {
        reason: "daemon upgrade left the scope unverified".into(),
    }
}

pub(super) async fn possible_scope_upgrade(
    guard: &ToolOpenGuard,
    cancel: &CancellationToken,
) -> Option<Instant> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => None,
        deadline = crate::daemon_upgrade::possible_deadline(guard.deadline) => deadline,
    }
}

pub(super) async fn wait_for_resumed_scope(
    upgrade: Instant,
    guard: &ToolOpenGuard,
    cancel: &CancellationToken,
) -> bool {
    let until = guard
        .deadline
        .map_or(upgrade, |deadline| deadline.min(upgrade));
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        result = tokio::time::timeout_at(until, crate::daemon_upgrade::wait_for_module(upgrade)) => result.unwrap_or(false),
    }
}

impl SubcToolPlane {
    pub(super) async fn read_scope_after_cut(
        &self,
        scope: &SessionScope,
        cancel: &CancellationToken,
        guard: &ToolOpenGuard,
        upgrade: Option<Instant>,
    ) -> ScopeReading {
        match upgrade {
            None => self.read_scope(scope, cancel, guard.deadline, true).await,
            Some(upgrade) => {
                let until = guard
                    .deadline
                    .map_or(upgrade, |deadline| deadline.min(upgrade));
                tokio::time::timeout_at(until, self.read_scope(scope, cancel, Some(until), true))
                    .await
                    .unwrap_or_else(|_| {
                        ScopeReading::Unverifiable(
                            "scope read did not finish before the daemon upgrade deadline".into(),
                        )
                    })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn finish_or_retry_upgrade(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        module: &str,
        fence_key: FenceKey,
        mut route: RouteHandle,
        composed_under: Option<ScopeStamp>,
        mut outcome: Result<serde_json::Value, ConnError>,
        key: Option<broca_core::tool_plane::DispatchKey<'_>>,
        mut permit: tokio::sync::OwnedSemaphorePermit,
        guard: &ToolOpenGuard,
    ) -> DispatchResult {
        let mut upgrade = Retry::default();
        let scope = ctx.scope.as_ref();
        let mut resend_turn = None;
        while refused(&outcome) {
            let route_key = Self::route_key(&ctx.bind, scope, module);
            resend_turn.get_or_insert_with(|| self.begin_resend_turn(&route_key));
            drop(permit);
            self.forget_route(ctx, module, route);
            let lifecycle = self.capture_session_lifecycle_token(&ctx.bind);
            let wait = WaitCancel::link(&lifecycle, guard);
            let ready = tokio::select! {
                biased;
                _ = wait.token().cancelled() => return self.stopped_before_send(ctx, call),
                _ = async {
                    match guard.deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => false,
                ready = upgrade.wait(true, self.conn.upgrade_connection()) => ready,
            };
            if !ready {
                return DispatchResult::NotSent {
                    reason: "daemon upgrade refused the tool call before delivery".into(),
                };
            }
            let ceiling = upgrade
                .deadline()
                .expect("a successful wait has an upgrade deadline");
            let until = guard.deadline.map_or(ceiling, |end| ceiling.min(end));
            let guarded = guard.clone().with_deadline(Some(until));
            let open = self.ensure_result_route_for_lifecycle_inner(
                &ctx.bind,
                scope,
                module,
                &lifecycle,
                Some(until),
                &guarded,
                TOOL_DISPATCH_PATIENCE,
                self.frozen_contract(ctx, module).route_shape(),
            );
            let (fresh, sem) = match tokio::time::timeout_at(until, open).await {
                Ok(Ok(opened)) => opened,
                _ if guard.interrupted() => return self.stopped_before_send(ctx, call),
                Ok(Err(error)) => {
                    let error = error.into_plane();
                    if let Some(reason) = error
                        .ended_scope_reason()
                        .filter(|reason| *reason != SCOPE_UNVERIFIABLE)
                    {
                        self.latch_open_refusal(ctx, &error);
                        return DispatchResult::ScopeDrained {
                            reason: reason.to_owned(),
                            sent: false,
                        };
                    }
                    return DispatchResult::NotSent {
                        reason: format!("tool route did not reopen after daemon upgrade: {error}"),
                    };
                }
                Err(_) => {
                    return DispatchResult::NotSent {
                        reason: "tool route did not reopen by the daemon upgrade deadline".into(),
                    }
                }
            };
            if let Some(scope) = scope {
                let stamp = self.cached_stamp(&ctx.bind, Some(scope), module, fresh);
                match composed_under.as_ref().zip(stamp.as_ref()) {
                    Some((old, new)) if may_resend_under(old, new) => {}
                    Some(_) => {
                        if self.take_route_if_owned(&route_key, fresh).is_some() {
                            self.close_route_best_effort(fresh);
                        }
                        return DispatchResult::ScopeDrained {
                            reason: SCOPE_AGENT_CHANGED.into(),
                            sent: false,
                        };
                    }
                    None => {
                        return DispatchResult::NotSent {
                            reason: "daemon upgrade left the scope unverified".into(),
                        }
                    }
                }
            }
            permit = tokio::select! {
                biased;
                _ = wait.token().cancelled() => return self.stopped_before_send(ctx, call),
                _ = tokio::time::sleep_until(until) => return DispatchResult::NotSent { reason: "daemon upgrade deadline passed before tool send".into() },
                permit = sem.acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => return DispatchResult::NotSent { reason: "route semaphore closed".into() },
                },
            };
            let reply_deadline = self.reply_deadline(ctx, module, call);
            outcome = match self
                .send_call(
                    ctx,
                    module,
                    &fresh,
                    call,
                    reply_deadline,
                    key,
                    &guarded.signals(),
                )
                .await
            {
                SendAttempt::NotSent { reason } => return DispatchResult::NotSent { reason },
                SendAttempt::Reply(reply) => reply,
            };
            route = fresh;
        }
        self.finish_dispatch(
            ctx,
            call,
            module,
            fence_key,
            route,
            composed_under,
            outcome,
            key,
            permit,
        )
        .await
    }
}
