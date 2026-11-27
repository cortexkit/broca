//! Route opens can be retried before they carry any application request.

use super::*;
use crate::daemon_upgrade::{active_deadline, Retry};
use tokio::time::Instant;

impl SubcConnection {
    /// Capabilities describe the replacement socket, not the socket lost during cutover.
    pub(crate) async fn scoped_server_capabilities(
        &self,
        deadline: Instant,
    ) -> ConnResult<Vec<String>> {
        self.read_during_upgrade(deadline, "scope capability discovery", || {
            self.server_capabilities()
        })
        .await
    }

    /// A read of daemon metadata may have lost its reply at cutover. Read it again on the
    /// replacement generation, within the caller's budget, rather than using stale metadata.
    /// This is not for provider calls: loss of their replies is not proof of non-delivery.
    pub(super) async fn read_during_upgrade<T, F, Fut>(
        &self,
        deadline: Instant,
        operation: &'static str,
        mut read: F,
    ) -> ConnResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = ConnResult<T>>,
    {
        let before = self.shared.current.lock().unwrap().runtime.conn;
        let result = read().await;
        if !matches!(result, Err(ConnError::Transport(_))) {
            return result;
        }
        let lost_generation = {
            let current = self.shared.current.lock().unwrap();
            !current.active || current.runtime.conn != before
        };
        if !lost_generation && active_deadline().is_none() {
            return result;
        }
        let Some(upgrade) = crate::daemon_upgrade::possible_deadline(Some(deadline)).await else {
            return result;
        };
        let until = upgrade.min(deadline);
        time::timeout_at(until, async {
            if crate::daemon_upgrade::wait_for_module(upgrade).await
                && self.wait_for_active_generation(until).await
            {
                read().await
            } else {
                result
            }
        })
        .await
        .unwrap_or_else(|_| {
            Err(ConnError::Transport(format!(
                "{operation} did not finish before its deadline"
            )))
        })
    }

    pub(crate) async fn wait_for_active_generation(&self, deadline: Instant) -> bool {
        while Instant::now() < deadline {
            if self.shared.current.lock().unwrap().active {
                return true;
            }
            time::sleep_until((Instant::now() + Duration::from_millis(10)).min(deadline)).await;
        }
        false
    }

    async fn route_open_wire_target_once(
        &self,
        target: RouteTarget,
        identity: subc_protocol::BindIdentity,
        scope: Option<subc_protocol::scope::ScopeSelector>,
        role_versions: Option<std::collections::BTreeMap<String, String>>,
        consumer_capabilities: Option<Vec<String>>,
    ) -> ConnResult<RouteHandle> {
        #[cfg(feature = "test-clock")]
        if let Some(starts) = &self.route_open_attempts {
            starts.lock().unwrap().push(time::Instant::now());
        }
        let reads_held_reply = cortexkit_role_tool_provider::consumer_reads_held_reply_v1(
            consumer_capabilities.as_deref(),
        );
        let reads_tool_result = cortexkit_role_tool_provider::consumer_reads_tool_result_v1(
            consumer_capabilities.as_deref(),
        );
        let call_keepalive = consumer_capabilities.as_ref().is_some_and(|caps| {
            caps.iter()
                .any(|cap| cap == cortexkit_role_tool_provider::CALL_KEEPALIVE_CAPABILITY)
        });
        let declares_call_yield = cortexkit_role_tool_provider::consumer_reads_call_yield_v1(
            consumer_capabilities.as_deref(),
        );
        let tool_provider_v1 = role_versions
            .as_ref()
            .is_some_and(|roles| roles.contains_key("tool-provider"));
        let body = |runtime: &ConnectionRuntime| {
            // Select the identity from the same runtime that will enqueue the request. A
            // replacement socket may use different authentication from the preceding one.
            let req = ClientControlRequest::RouteOpen {
                identity,
                target,
                consumer_identity: if runtime.binding == crate::registration::SocketBinding::Claim {
                    None
                } else {
                    self.shared.consumer_identity.clone()
                },
                // Only dispatch routes negotiate result bodies. Role discovery and management
                // routes keep their previous undeclared format; held replies are a separate opt-in.
                consumer_capabilities,
                // This consumer has no scope admission facts to send.
                admission_facts: None,
                // Scoped tool and management clients first check the daemon advertises `scopes/v1`:
                // an older daemon drops the field and would open an unscoped route instead.
                scope,
                // Tool routes declare the role only when the daemon manifest lists it.
                role_versions,
                // The daemon admits only so many requests in flight on one route. Leaving this unset
                // keeps the old behaviour: a request past that limit waits for a slot instead of
                // being refused at once with `route_busy`.
                busy_refusals: None,
            };
            serde_json::to_vec(&req).map_err(|e| ConnError::Transport(e.to_string()))
        };
        let (frame, conn) = self
            .route_open_rpc_deadline(body, ROUTE_OPEN_ATTEMPT_TIMEOUT)
            .await?;
        match frame.header.ty {
            FrameType::Response => {
                match serde_json::from_slice::<ClientControlResponse>(&frame.body) {
                    Ok(ClientControlResponse::RouteOpen {
                        route_channel,
                        route_epoch,
                        ..
                    }) => {
                        let mut handle = RouteHandle::new(route_channel, route_epoch, conn);
                        handle.reads_tool_result = reads_tool_result;
                        handle.reads_held_reply = reads_held_reply;
                        handle.declares_call_yield = declares_call_yield;
                        handle.call_keepalive = call_keepalive;
                        handle.tool_provider_v1 = tool_provider_v1;
                        Ok(handle)
                    }
                    Ok(other) => Err(ConnError::Protocol(format!(
                        "unexpected route.open reply: {other:?}"
                    ))),
                    Err(e) => Err(ConnError::Protocol(format!("decode route.open: {e}"))),
                }
            }
            FrameType::Error => Err(ConnError::Rejected(decode_error_body(&frame.body))),
            ty => Err(ConnError::Protocol(format!(
                "unexpected route.open frame {ty:?}"
            ))),
        }
    }

    pub(super) async fn route_open_during_upgrade(
        &self,
        target: RouteTarget,
        identity: subc_protocol::BindIdentity,
        scope: Option<subc_protocol::scope::ScopeSelector>,
        role_versions: Option<std::collections::BTreeMap<String, String>>,
        capabilities: Option<Vec<String>>,
    ) -> ConnResult<RouteHandle> {
        let mut retry = Retry::default();
        loop {
            let deadline = retry.deadline().or_else(active_deadline);
            if let Some(deadline) = deadline {
                if !self.wait_for_active_generation(deadline).await {
                    return Err(ConnError::Transport(
                        "daemon upgrade reconnect deadline passed".into(),
                    ));
                }
            }
            let attempt = self.route_open_wire_target_once(
                target.clone(),
                identity.clone(),
                scope.clone(),
                role_versions.clone(),
                capabilities.clone(),
            );
            let outcome = match deadline {
                Some(deadline) => time::timeout_at(deadline, attempt)
                    .await
                    .unwrap_or_else(|_| {
                        Err(ConnError::Transport(
                            "route.open did not finish by the daemon upgrade deadline".into(),
                        ))
                    }),
                None => attempt.await,
            };
            if let Err(error) = &outcome {
                if retry.after_error(error, self).await {
                    continue;
                }
            }
            return outcome;
        }
    }
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn route_open_waits_through_the_daemon_upgrade_refusal() {
    // The upgrade notice is process-wide and would affect other tests' route opens.
    if !crate::daemon_upgrade::run_in_test_child(
        module_path!(),
        "route_open_waits_through_the_daemon_upgrade_refusal",
    ) {
        return;
    }
    let (conn, _, mut daemon) = SubcConnection::in_process_for_test(7, 1);
    let bind = BindIdentity::new("/project", "broca", "upgrade");
    let open = conn.route_open_management("target", &bind);
    tokio::pin!(open);
    let first = tokio::select! {
        request = daemon.next_request() => request,
        result = &mut open => panic!("open ended before its request: {result:?}"),
    };
    let ceiling = Instant::now() + Duration::from_secs(2);
    crate::daemon_upgrade::announce(ceiling);
    assert!(daemon.refuse_for_daemon_upgrade(&first));
    assert!(
        futures::poll!(&mut open).is_pending(),
        "the upgrade refusal must not end the open"
    );
    crate::daemon_upgrade::resumed(ceiling);
    let retry = tokio::select! {
        request = daemon.next_request() => request,
        result = &mut open => panic!("upgrade refusal was not retried: {result:?}"),
    };
    assert_eq!(retry.body, first.body);
    assert!(daemon.reply(
        &retry,
        br#"{"op":"route.open","route_channel":7,"route_epoch":1}"#.to_vec()
    ));
    assert_eq!(open.await.unwrap().channel, 7);
    crate::daemon_upgrade::abandon();
}

#[cfg(test)]
#[tokio::test(start_paused = true)]
async fn a_nonce_socket_keeps_its_wire_identity_after_the_module_resumes() {
    // Resume installs a process-wide claim minter and module identity that other tests would use.
    if !crate::daemon_upgrade::run_in_test_child(
        module_path!(),
        "a_nonce_socket_keeps_its_wire_identity_after_the_module_resumes",
    ) {
        return;
    }
    struct NeverMint;
    #[async_trait::async_trait]
    impl crate::registration::ClaimMinter for NeverMint {
        async fn call(
            &self,
            _: &subc_protocol::claim::ClaimMintRequest,
        ) -> Result<crate::registration::MintAnswer, String> {
            panic!("this socket authenticated before the new minter was installed");
        }
    }

    let (mut conn, _, mut peer) = SubcConnection::in_process_for_test(7, 1);
    Arc::get_mut(&mut conn.shared).unwrap().consumer_identity =
        Some(subc_control::ConsumerIdentity {
            module_id: "broca".into(),
            launch_nonce: "test-nonce".into(),
        });
    crate::registration::install_resumed_minter("broca", Arc::new(NeverMint));
    let bind = BindIdentity::new("/project", "broca", "nonce-socket");
    let open = conn.route_open_management("target", &bind);
    tokio::pin!(open);
    let request = tokio::select! {
        sent = peer.next_request() => sent,
        done = &mut open => panic!("route.open did not reach the nonce socket: {done:?}"),
    };
    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(
        body["consumer_identity"]["module_id"], "broca",
        "a socket that never presented a claim still needs its nonce identity"
    );
}
