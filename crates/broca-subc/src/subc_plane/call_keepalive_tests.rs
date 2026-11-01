mod call_keepalive {
    use super::*;
    use crate::connection::InProcessModuleEnd;
    use broca_core::{
        run_loop_with_control, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunControl,
        RunSummary, StubProvider,
    };
    use broca_protocol::caller::{ControlUnit, RunFinishReason};
    use subc_protocol::FrameType;

    pub(super) async fn bound_request(
        module: &mut InProcessModuleEnd,
    ) -> (subc_protocol::Frame, bool) {
        let bind = module.next_request().await;
        let raw: serde_json::Value = serde_json::from_slice(&bind.body).unwrap();
        assert_eq!(raw["op"], "route.open");
        let keepalive = raw["consumer_capabilities"]
            .as_array()
            .is_some_and(|caps| caps.iter().any(|cap| cap == "call-keepalive/v1"));
        let answer = subc_control::ClientControlResponse::RouteOpen {
            route_channel: 7,
            route_epoch: 1,
        };
        assert!(module.reply(&bind, serde_json::to_vec(&answer).unwrap()));
        (module.next_request().await, keepalive)
    }

    fn frozen_manifest(keepalive: bool, reply: Option<u64>) -> broca_wal::PlanManifest {
        let tool = serde_json::json!({
            "name":"read", "description":"probe", "input_schema": {"type":"object"},
            "module":"aft", "execution_mode":"parallel"
        });
        let mut fetch =
            serde_json::json!({"provider":"aft", "catalog_digest":"digest", "tools":[tool]});
        if keepalive {
            fetch["declarations_frozen"] = serde_json::json!({"call_keepalive":true});
        }
        if let Some(max_ms) = reply {
            fetch["reply"] = serde_json::json!({"read":{"max_ms":max_ms}});
        }
        let manifest: broca_wal::PlanManifest = serde_json::from_value(serde_json::json!({
            "plan":r#"{"tool_items":[{"provider":"aft"}]}"#,
            "plan_identity":"identity", "composition_digest":"composition",
            "fetches":[fetch], "tools":[tool], "system_text":"", "system_digest":"",
            "join_version":1, "role_versions":{}
        }))
        .unwrap();
        // Resume reads only the stored promise, not the provider's current catalog.
        serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap()
    }

    fn rig() -> (SubcToolPlane, InProcessModuleEnd) {
        let (conn, route, module) = SubcConnection::in_process_for_test(7, 1);
        let plane = SubcToolPlane::with_connection(
            Arc::new(InProcessWireConnection { conn, route }),
            "aft",
        );
        plane.windows.lock().unwrap().insert("aft".into(), 1);
        (plane, module)
    }

    async fn run(
        tag: &str,
        plane: &SubcToolPlane,
        mut control: RunControl,
        manifest: broca_wal::PlanManifest,
    ) -> (RunSummary, Vec<OutputEvent>) {
        let root = broca_test_support::scratch_root().join(format!("call-wait-{tag}"));
        std::fs::create_dir_all(&root).unwrap();
        let request = outage_wait_tests::request(tag, &root);
        let provider = StubProvider::new(vec![
            outage_wait_tests::tool_step(),
            outage_wait_tests::text_step("done"),
        ]);
        let leases = FileLeaseStore::new(root.join("leases"));
        let wal = FileWalStore::new(root.join("wal"));
        control.manifest = Some(Arc::new(manifest));
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        let collector = tokio::spawn(async move {
            let mut events = Vec::new();
            while let Some(event) = rx.recv().await {
                events.push(event);
            }
            events
        });
        let summary = run_loop_with_control(
            request, &provider, plane, &leases, &AllowAll, &wal, None, control, &tx,
        )
        .await
        .unwrap();
        drop(tx);
        let events = collector.await.unwrap();
        std::fs::remove_dir_all(&root).unwrap();
        (summary, events)
    }

    fn results(events: &[OutputEvent]) -> Vec<&ToolResult> {
        events
            .iter()
            .filter_map(|event| match event {
                OutputEvent::Control(ControlUnit::ToolResult { result }) => Some(result),
                _ => None,
            })
            .collect()
    }

    fn assert_cancel_and_ignore_late(
        module: &mut InProcessModuleEnd,
        request: &subc_protocol::Frame,
    ) {
        let frames = module.take_written_for_test();
        let cancels: Vec<_> = frames
            .iter()
            .filter(|f| f.header.ty == FrameType::Cancel)
            .collect();
        assert_eq!(cancels.len(), 1, "{frames:?}");
        assert_eq!(cancels[0].header.corr, request.header.corr);
        assert_eq!(cancels[0].header.channel, request.header.channel);
        assert_eq!(cancels[0].header.epoch, request.header.epoch);
        assert!(
            !frames.iter().any(|f| f.header.ty == FrameType::Request),
            "no re-send"
        );
        assert_eq!(module.pending_count_for_test(), 0);
        assert!(!module.push(
            request,
            cortexkit_role_tool_provider::keepalive_body().to_vec()
        ));
        assert!(!module.reply(
            request,
            br#"{"content":[{"type":"text","text":"late"}]}"#.to_vec()
        ));
    }

    #[tokio::test]
    async fn kit_keepalive_deadline_vectors_reach_frozen_dispatch_selection() {
        let vectors: serde_json::Value = serde_json::from_str(
            cortexkit_role_tool_provider_conformance::wire::vectors::CALL_KEEPALIVE_JSON,
        )
        .unwrap();
        for case in vectors["deadlines"].as_array().unwrap() {
            for keepalive in [false, true] {
                let mut frozen = serde_json::to_value(frozen_manifest(keepalive, None)).unwrap();
                if !case["reply"].is_null() {
                    frozen["fetches"][0]["reply"] = serde_json::json!({"read":case["reply"]});
                }
                let frozen: broca_wal::PlanManifest = serde_json::from_value(frozen).unwrap();
                let manifest = ToolManifest::from_tools(frozen.tools.as_ref().clone())
                    .with_plan_manifest(&frozen)
                    .unwrap();
                let conn = ScriptedToolPlaneConnection::new(vec![vec![]], vec![]);
                let plane = SubcToolPlane::with_connection(conn, "aft")
                    .with_reply_fallback_for_test(case["runner_fallback_ms"].as_u64().unwrap());
                plane.windows.lock().unwrap().insert("aft".into(), 1);
                let ctx = batch_ctx("vectors");
                let mut call = call("read");
                call.input = case["arguments"].clone();
                plane
                    .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                    .await
                    .unwrap();
                assert_eq!(plane.frozen_contract(&ctx, "aft").call_keepalive, keepalive);
                let expected = if keepalive {
                    "with_keepalive_ms"
                } else {
                    "without_keepalive_ms"
                };
                assert_eq!(
                    plane.reply_deadline(&ctx, "aft", &call),
                    Duration::from_millis(case[expected].as_u64().unwrap()),
                    "{} (keepalive={keepalive})",
                    case["name"],
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn advancing_call_beyond_120_seconds_records_one_result_and_no_keepalive_display() {
        let (plane, mut module) = rig();
        let provider = async {
            let (request, keepalive) = bound_request(&mut module).await;
            assert!(keepalive);
            let started = Instant::now();
            for _ in 0..6 {
                sleep(Duration::from_secs(30)).await;
                assert!(module.push(
                    &request,
                    br#"{"kind":"keepalive","private_payload":"never_display_me"}"#.to_vec()
                ));
            }
            assert!(started.elapsed() > Duration::from_secs(120));
            assert!(module.reply(
                &request,
                serde_json::to_vec(&serde_json::json!({
                    "content":[{"type":"text","text":"answer"}], "isError":false
                }))
                .unwrap()
            ));
        };
        let (run, ()) = tokio::join!(
            run(
                "healthy",
                &plane,
                RunControl::default(),
                frozen_manifest(true, None)
            ),
            provider
        );
        let (summary, events) = run;
        assert_eq!(summary.reason, RunFinishReason::Completed);
        let recorded = results(&events);
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].output,
            ToolOutput::Text {
                text: "answer".into()
            }
        );
        let visible = format!("{events:?}");
        assert!(!visible.contains("never_display_me"));
        assert!(!visible.contains("\"keepalive\""));
        assert!(!module
            .take_written_for_test()
            .iter()
            .any(|f| f.header.ty == FrameType::Cancel));
    }

    #[tokio::test(start_paused = true)]
    async fn silent_calls_are_outcome_unknown_cancelled_and_never_replied_twice() {
        for keepalive in [false, true] {
            let (plane, mut module) = rig();
            let provider = bound_request(&mut module);
            let (run, (request, declared)) = tokio::join!(
                run(
                    "silent",
                    &plane,
                    RunControl::default(),
                    frozen_manifest(keepalive, None)
                ),
                provider,
            );
            assert_eq!(declared, keepalive);
            assert_eq!(run.0.reason, RunFinishReason::Interrupted);
            let recorded = results(&run.1);
            assert_eq!(recorded.len(), 1);
            assert!(recorded[0].is_error);
            assert_cancel_and_ignore_late(&mut module, &request);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_declared_reply_deadline_cancels_advancing_work_through_core() {
        let (plane, mut module) = rig();
        let provider = async {
            let (request, keepalive) = bound_request(&mut module).await;
            assert!(keepalive);
            for _ in 0..3 {
                sleep(Duration::from_secs(30)).await;
                assert!(module.push(
                    &request,
                    cortexkit_role_tool_provider::keepalive_body().to_vec()
                ));
            }
            request
        };
        let (run, request) = tokio::join!(
            run(
                "deadline",
                &plane,
                RunControl::default(),
                frozen_manifest(true, Some(120_000))
            ),
            provider,
        );
        assert_eq!(run.0.reason, RunFinishReason::Interrupted);
        assert_eq!(results(&run.1).len(), 1);
        assert!(format!("{:?}", results(&run.1)).contains("timed out after 120s"));
        assert_cancel_and_ignore_late(&mut module, &request);
    }

    #[tokio::test(start_paused = true)]
    async fn run_cancel_and_shutdown_cancel_every_outstanding_request() {
        for keepalive in [false, true] {
            for shutdown in [false, true] {
                let (plane, mut module) = rig();
                let signal = CancellationToken::new();
                let control = if shutdown {
                    RunControl {
                        stop_requested: Some(signal.clone()),
                        ..RunControl::default()
                    }
                } else {
                    RunControl {
                        cancel: Some(signal.clone()),
                        ..RunControl::default()
                    }
                };
                let provider = async {
                    let (request, declared) = bound_request(&mut module).await;
                    assert_eq!(declared, keepalive);
                    signal.cancel();
                    request
                };
                let (run, request) = tokio::join!(
                    run("stopped", &plane, control, frozen_manifest(keepalive, None)),
                    provider,
                );
                // A sent call without an answer is ambiguous even when cancellation ended
                // the wait. Core preserves that outcome rather than claiming it never ran.
                assert_eq!(run.0.reason, RunFinishReason::Interrupted);
                let recorded = results(&run.1);
                assert_eq!(recorded.len(), 1);
                assert!(recorded[0].is_error);
                assert!(format!("{recorded:?}").contains("run cancelled or stopped"));
                assert_cancel_and_ignore_late(&mut module, &request);
            }
        }
    }
}
