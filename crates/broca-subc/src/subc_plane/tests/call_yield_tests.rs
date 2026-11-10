use super::*;
use crate::connection::{ApprovedCallError, InProcessModuleEnd, ToolReplyDeadline};
use broca_core::tool_plane::{DispatchSignals, ToolOpenGuard};
use subc_protocol::FrameType;

/// Keep the real request, reply, and abandonment machinery; only route discovery is local.
struct Wire {
    conn: SubcConnection,
    route: RouteHandle,
}

#[async_trait]
impl ToolPlaneConnection for Wire {
    fn consumer_module_id(&self) -> Option<&str> {
        Some("broca")
    }
    async fn catalog_list(&self) -> Result<Vec<CatalogEntry>, ConnError> {
        Ok(Vec::new())
    }
    async fn route_open(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&ScopeSelector>,
    ) -> Result<RouteHandle, ConnError> {
        Ok(self.route)
    }
    async fn route_open_dispatch(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&ScopeSelector>,
        _: RouteShape,
        installed: bool,
    ) -> Result<RouteHandle, ConnError> {
        let mut route = self.route;
        route.reads_tool_result = installed;
        route.reads_held_reply = installed;
        route.declares_call_yield = installed;
        Ok(route)
    }
    async fn route_close(&self, _: &RouteHandle) -> Result<(), ConnError> {
        Ok(())
    }
    fn route_is_open(&self, _: &RouteHandle) -> bool {
        true
    }
    async fn server_capabilities(&self) -> Result<Vec<String>, ConnError> {
        Ok(Vec::new())
    }
    async fn tool_call(
        &self,
        _: &RouteHandle,
        _: &str,
        _: &serde_json::Value,
        _: Option<&str>,
        _: Duration,
        _: Option<&str>,
    ) -> Result<serde_json::Value, ConnError> {
        unreachable!()
    }
    async fn tool_call_staged(
        &self,
        route: &RouteHandle,
        call: &ToolCall,
        deadline: ToolReplyDeadline,
        preset: Option<&str>,
        key: Option<&str>,
        pin: Option<&str>,
        signals: &DispatchSignals,
    ) -> Result<serde_json::Value, ApprovedCallError> {
        self.conn
            .tool_call_raw_yieldable_guarded(
                route,
                &call.tool_name,
                &call.input.to_string(),
                Some(&call.tool_call_id.0),
                deadline.duration,
                deadline.declared,
                preset,
                key,
                pin,
                signals,
                deadline.yield_provider.as_deref(),
            )
            .await
    }
}

fn wire() -> (Arc<Wire>, InProcessModuleEnd) {
    let (conn, route, module) = SubcConnection::in_process_for_test(7, 1);
    (Arc::new(Wire { conn, route }), module)
}

#[tokio::test]
async fn stop_yields_once_on_the_original_route_and_held_final_never_cancels() {
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
    for final_held in [false, true] {
        let (wire, mut provider) = wire();
        let rig = Rig::new(true, true, true, true);
        let plane = SubcToolPlane::with_connection(wire, "provider")
            .with_provider_held_support(rig.clone(), rig.clone());
        let ctx = ctx();
        let call = call("call");
        plane.batch_started(&ctx, std::slice::from_ref(&call), &manifest_with_yield(Some("tool-result/v1"), true, true)).await.unwrap();
        plane.register_dispatch_group(&ctx, &[registration(&call, "lineage:1", false)]).await.unwrap();
        let stop = CancellationToken::new();
        let guard = ToolOpenGuard::new(None, Some(&stop));
        let mut dispatch = Box::pin(plane.dispatch_guarded(&ctx, &call, Some(DispatchKey { call_key: "lineage:1", held_recorder: Some(rig.as_ref()) }), &guard));
        assert!(futures::poll!(&mut dispatch).is_pending());
        let original = provider.next_request().await;
        stop.cancel();
        assert!(futures::poll!(&mut dispatch).is_pending());
        let yielding = provider.next_request().await;
        assert_eq!((yielding.header.channel, yielding.header.epoch), (original.header.channel, original.header.epoch));
        let body: serde_json::Value = serde_json::from_slice(&yielding.body).unwrap();
        assert_eq!(body["name"], "tool.yield");
        assert_eq!(body["arguments"], json!({"call_key":"lineage:1","reason":"runner_restart"}));
        assert!(body.get("call_key").is_none());
        assert!(provider.reply(&yielding, br#"{"answer":"accepted"}"#.to_vec()));
        assert!(futures::poll!(&mut dispatch).is_pending());
        assert!(futures::poll!(&mut dispatch).is_pending());
        assert!(provider.try_frame().is_none(), "stop emits exactly one yield");
        let final_frame = if final_held {
            json!({"content":[{"type":"text","text":"running"}],"held":{"reason":"running","yielded":true,"detail":{"id":"opaque"}}})
        } else { json!({"content":[{"type":"text","text":"ready"}]}) };
        let decoded: cortexkit_role_tool_provider::result::ToolResult = serde_json::from_value(final_frame.clone()).unwrap();
        cortexkit_role_tool_provider::result::validate(&decoded).unwrap();
        assert!(provider.reply(&original, serde_json::to_vec(&final_frame).unwrap()));
        let result = completed(dispatch.await);
        assert_eq!(result.output, ToolOutput::Text { text: (if final_held { "late terminal" } else { "ready" }).into() });
        assert_eq!(rig.held_synced.load(Ordering::SeqCst), final_held);
        assert!(provider.try_frame().is_none(), "a final frame disarms CANCEL");
    }
    }).await.unwrap();
}

#[tokio::test]
async fn refused_or_silent_yield_keeps_abort_and_cancel() {
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        for answer in ["not_yieldable", "unknown_call", "accepted"] {
            let (wire, mut provider) = wire();
            let rig = Rig::new(true, true, true, true);
            let plane = SubcToolPlane::with_connection(wire, "provider")
                .with_provider_held_support(rig.clone(), rig.clone());
            let ctx = ctx();
            let call = call("call");
            plane
                .batch_started(
                    &ctx,
                    std::slice::from_ref(&call),
                    &manifest_with_yield(Some("tool-result/v1"), true, true),
                )
                .await
                .unwrap();
            plane
                .register_dispatch_group(&ctx, &[registration(&call, "lineage:1", false)])
                .await
                .unwrap();
            let stop = CancellationToken::new();
            let guard = ToolOpenGuard::new(None, Some(&stop));
            let mut dispatch = Box::pin(plane.dispatch_guarded(
                &ctx,
                &call,
                Some(DispatchKey {
                    call_key: "lineage:1",
                    held_recorder: Some(rig.as_ref()),
                }),
                &guard,
            ));
            assert!(futures::poll!(&mut dispatch).is_pending());
            let original = provider.next_request().await;
            stop.cancel();
            assert!(futures::poll!(&mut dispatch).is_pending());
            let yielding = provider.next_request().await;
            assert!(provider.reply(
                &yielding,
                serde_json::to_vec(&json!({"answer":answer})).unwrap()
            ));
            assert!(futures::poll!(&mut dispatch).is_pending());
            drop(dispatch);
            let cancel = provider
                .try_frame()
                .expect("grace expiry abandons original wait");
            assert_eq!(cancel.header.ty, FrameType::Cancel);
            assert_eq!(cancel.header.corr, original.header.corr);
            assert!(provider.try_frame().is_none());
            assert!(!rig.held_synced.load(Ordering::SeqCst));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_frozen_yield_facts_never_send_yield() {
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        for (format, late, tagged) in [
            (Some("tool-result/v1"), true, false),
            (Some("tool-result/v1"), false, true),
            (Some("content/v1"), true, true),
        ] {
            let (wire, mut provider) = wire();
            let rig = Rig::new(true, true, true, late);
            let plane = SubcToolPlane::with_connection(wire, "provider")
                .with_provider_held_support(rig.clone(), rig.clone());
            let ctx = ctx();
            let call = call("call");
            plane
                .batch_started(
                    &ctx,
                    std::slice::from_ref(&call),
                    &manifest_with_yield(format, late, tagged),
                )
                .await
                .unwrap();
            plane
                .register_dispatch_group(&ctx, &[registration(&call, "lineage:1", false)])
                .await
                .unwrap();
            let stop = CancellationToken::new();
            let guard = ToolOpenGuard::new(None, Some(&stop));
            let mut dispatch = Box::pin(plane.dispatch_guarded(
                &ctx,
                &call,
                Some(DispatchKey {
                    call_key: "lineage:1",
                    held_recorder: Some(rig.as_ref()),
                }),
                &guard,
            ));
            assert!(futures::poll!(&mut dispatch).is_pending());
            let original = provider.next_request().await;
            stop.cancel();
            assert!(futures::poll!(&mut dispatch).is_pending());
            assert!(provider.try_frame().is_none());
            drop(dispatch);
            let cancel = provider.try_frame().unwrap();
            assert_eq!(cancel.header.ty, FrameType::Cancel);
            assert_eq!(cancel.header.corr, original.header.corr);
        }
    })
    .await
    .unwrap();
}
