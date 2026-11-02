mod call_keepalive {
    use super::*;
    use crate::connection::InProcessModuleEnd;
    use broca_core::{
        run_loop_with_control, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunControl,
        RunSummary, StubProvider,
    };
    use broca_protocol::caller::{ControlUnit, RunFinishReason};
    use broca_wal::Wal;
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
        control: RunControl,
        manifest: broca_wal::PlanManifest,
    ) -> (RunSummary, Vec<OutputEvent>) {
        let root = broca_test_support::scratch_root().join(format!("call-wait-{tag}"));
        let provider = StubProvider::new(vec![
            outage_wait_tests::tool_step(),
            outage_wait_tests::text_step("done"),
        ]);
        let answer = run_at(&root, tag, plane, control, manifest, &provider, false).await;
        std::fs::remove_dir_all(&root).unwrap();
        answer
    }

    async fn run_at(
        root: &std::path::Path,
        tag: &str,
        plane: &SubcToolPlane,
        mut control: RunControl,
        manifest: broca_wal::PlanManifest,
        provider: &StubProvider,
        resume: bool,
    ) -> (RunSummary, Vec<OutputEvent>) {
        std::fs::create_dir_all(root).unwrap();
        let mut request = outage_wait_tests::request(tag, root);
        request.inherit_frozen_cache = resume;
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
            request, provider, plane, &leases, &AllowAll, &wal, None, control, &tx,
        )
        .await
        .unwrap();
        drop(tx);
        let events = collector.await.unwrap();
        (summary, events)
    }

    fn wal_records(root: &std::path::Path, tag: &str) -> Vec<broca_wal::FramedRecord> {
        let wal = FileWalStore::new(root.join("wal"));
        let request = outage_wait_tests::request(tag, root);
        broca_wal::FileWal::open(wal.wal_path(&request.session))
            .unwrap()
            .replay()
            .unwrap()
    }

    fn assert_one_unknown_terminal(records: &[broca_wal::FramedRecord]) {
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r.record, broca_wal::WalRecord::ToolDispatchIntent { .. }))
                .count(),
            1
        );
        let terminals: Vec<_> = records
            .iter()
            .filter_map(|r| match &r.record {
                broca_wal::WalRecord::ToolResult {
                    result, outcome, ..
                } => Some((result, outcome)),
                _ => None,
            })
            .collect();
        assert_eq!(terminals.len(), 1);
        let intent = records
            .iter()
            .find_map(|r| match &r.record {
                broca_wal::WalRecord::ToolDispatchIntent { tool_call_id, .. } => Some(tool_call_id),
                _ => None,
            })
            .unwrap();
        assert_eq!(&terminals[0].0.tool_call_id, intent);
        assert_eq!(*terminals[0].1, broca_wal::DispatchOutcome::Unknown);
        assert!(terminals[0].0.is_error);
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

    #[tokio::test]
    async fn declared_aft_bounds_cover_blocking_bash_and_background_worker_watch_arguments() {
        for (name, input) in [
            ("bash", serde_json::json!({"command":"work","wait":true})),
            ("bash", serde_json::json!({"command":"work"})),
            (
                "bash_watch",
                serde_json::json!({"taskId":"task","background":true,"timeoutMs":600_000}),
            ),
            ("bash_watch", serde_json::json!({"taskId":"task"})),
        ] {
            let mut value = serde_json::to_value(frozen_manifest(false, None)).unwrap();
            value["tools"][0]["name"] = serde_json::json!(name);
            value["fetches"][0]["tools"][0]["name"] = serde_json::json!(name);
            value["fetches"][0]["reply"] = serde_json::json!({name:{"max_ms":2_040_000}});
            let plan: broca_wal::PlanManifest = serde_json::from_value(value).unwrap();
            let manifest = ToolManifest::from_tools(plan.tools.as_ref().clone())
                .with_plan_manifest(&plan)
                .unwrap();
            let conn =
                ScriptedToolPlaneConnection::new(vec![vec![]], vec![successful_tool_reply("done")]);
            let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
            plane.windows.lock().unwrap().insert("aft".into(), 1);
            let ctx = batch_ctx("aft-declared-bounds");
            let mut call = call(name);
            call.input = input;
            plane
                .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                .await
                .unwrap();
            assert!(matches!(
                plane.dispatch(&ctx, &call).await,
                DispatchResult::Completed(_)
            ));
            assert_eq!(
                *conn.dispatched_deadlines.lock().unwrap(),
                [Duration::from_millis(2_040_000)]
            );
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
    async fn timeout_and_cancel_reopen_and_resume_with_one_durable_terminal_and_no_dispatch() {
        for cancel in [false, true] {
            let tag = if cancel {
                "durable-cancel"
            } else {
                "durable-timeout"
            };
            let root = broca_test_support::scratch_root().join(tag);
            let (plane, mut module) = rig();
            let provider = StubProvider::new(vec![outage_wait_tests::tool_step()]);
            let signal = CancellationToken::new();
            let control = RunControl {
                cancel: cancel.then(|| signal.clone()),
                ..RunControl::default()
            };
            let tool = async {
                let (request, _) = bound_request(&mut module).await;
                if cancel {
                    signal.cancel();
                }
                request
            };
            let ((summary, _), request) = tokio::join!(
                run_at(
                    &root,
                    tag,
                    &plane,
                    control,
                    frozen_manifest(true, None),
                    &provider,
                    false
                ),
                tool,
            );
            assert_eq!(summary.reason, RunFinishReason::Interrupted);
            assert_eq!(provider.calls().len(), 1);
            assert_cancel_and_ignore_late(&mut module, &request);
            let records = wal_records(&root, tag);
            assert_one_unknown_terminal(&records);

            // Keep the actual timeout/cancel result but omit the later WAL records, as
            // a crash during close-out can. Recovery must retain that terminal, not execute again.
            let cut = records
                .iter()
                .position(|r| matches!(r.record, broca_wal::WalRecord::ToolResult { .. }))
                .unwrap()
                + 1;
            let wal = FileWalStore::new(root.join("wal"));
            let bind = outage_wait_tests::request(tag, &root).session;
            let path = wal.wal_path(&bind);
            std::fs::remove_file(&path).unwrap();
            {
                let mut file = broca_wal::FileWal::open(&path)
                    .unwrap()
                    .with_feature_floor(wal.feature_floor_path());
                for frame in &records[..cut] {
                    file.append(frame.fence, &frame.record, true).unwrap();
                }
            }
            for resumed_again in [false, true] {
                let before = std::fs::read(&path).unwrap();
                let (reopened_plane, mut reopened_module) = rig();
                let no_model = StubProvider::new(vec![]);
                let (summary, _) = run_at(
                    &root,
                    tag,
                    &reopened_plane,
                    RunControl::default(),
                    frozen_manifest(true, None),
                    &no_model,
                    true,
                )
                .await;
                assert_eq!(summary.reason, RunFinishReason::Interrupted);
                assert!(no_model.calls().is_empty());
                assert!(
                    reopened_module.take_written_for_test().is_empty(),
                    "recovery sends neither bind nor tool requests"
                );
                let reopened = wal_records(&root, tag);
                assert_one_unknown_terminal(&reopened);
                assert_eq!(
                    reopened
                        .iter()
                        .filter(|r| matches!(r.record, broca_wal::WalRecord::RunFinished { .. }))
                        .count(),
                    1
                );
                if resumed_again {
                    assert_eq!(std::fs::read(&path).unwrap(), before);
                }
            }
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_long_declared_reply_records_ordinary_progress_past_120_seconds() {
        let tag = "long-declared-progress";
        let root = broca_test_support::scratch_root().join(tag);
        let (plane, mut module) = rig();
        let provider = StubProvider::new(vec![
            outage_wait_tests::tool_step(),
            outage_wait_tests::text_step("done"),
        ]);
        let work = async {
            let (request, keepalive) = bound_request(&mut module).await;
            assert!(keepalive);
            for _ in 0..5 {
                sleep(Duration::from_secs(30)).await;
                assert!(module.push(
                    &request,
                    br#"{"kind":"progress","message":"advancing"}"#.to_vec()
                ));
            }
            assert!(module.reply(
                &request,
                br#"{"content":[{"type":"text","text":"answer"}],"isError":false}"#.to_vec()
            ));
        };
        let ((summary, _), ()) = tokio::join!(
            run_at(
                &root,
                tag,
                &plane,
                RunControl::default(),
                frozen_manifest(true, Some(200_000)),
                &provider,
                false
            ),
            work
        );
        assert_eq!(summary.reason, RunFinishReason::Completed);
        let records = wal_records(&root, tag);
        let results: Vec<_> = records
            .iter()
            .filter_map(|r| match &r.record {
                broca_wal::WalRecord::ToolResult {
                    result, outcome, ..
                } => Some((result, outcome)),
                _ => None,
            })
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(*results[0].1, broca_wal::DispatchOutcome::Completed);
        assert_eq!(
            results[0].0.output,
            ToolOutput::Text {
                text: "answer".into()
            }
        );
        assert!(!module
            .take_written_for_test()
            .iter()
            .any(|f| f.header.ty == FrameType::Cancel));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn module_restart_bind_wait_leaves_the_full_declared_budget_after_enqueue() {
        for keepalive in [false, true] {
            let (plane, mut module) = rig();
            let ctx = batch_ctx("restart-budget");
            let call = call("read");
            let plan = frozen_manifest(keepalive, Some(120_000));
            let manifest = ToolManifest::from_tools(plan.tools.as_ref().clone())
                .with_plan_manifest(&plan)
                .unwrap();
            let dispatch = async {
                plane
                    .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                    .await
                    .unwrap();
                plane.dispatch(&ctx, &call).await
            };
            let started = Instant::now();
            let work = async {
                loop {
                    let bind = module.next_request().await;
                    let raw: serde_json::Value = serde_json::from_slice(&bind.body).unwrap();
                    assert_eq!(raw["op"], "route.open");
                    if started.elapsed() < Duration::from_secs(25) {
                        assert!(module.refuse(&bind, "module_reloading"));
                    } else {
                        let answer = subc_control::ClientControlResponse::RouteOpen {
                            route_channel: 7,
                            route_epoch: 1,
                        };
                        assert!(module.reply(&bind, serde_json::to_vec(&answer).unwrap()));
                        break;
                    }
                }
                let request = module.next_request().await;
                let enqueued = Instant::now();
                for _ in 0..3 {
                    sleep(Duration::from_secs(30)).await;
                    assert!(module.push(&request, br#"{"kind":"progress"}"#.to_vec()));
                }
                sleep(Duration::from_secs(20)).await;
                assert!(module.reply(&request, br#"{"content":[],"isError":false}"#.to_vec()));
                enqueued
            };
            let (outcome, enqueued) = tokio::join!(dispatch, work);
            assert!(
                matches!(outcome, DispatchResult::Completed(ref r) if !r.is_error),
                "{outcome:?}"
            );
            assert_eq!(enqueued.elapsed(), Duration::from_secs(110));
            assert!(started.elapsed() >= Duration::from_secs(135));
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
    async fn declared_reply_is_absolute_without_keepalive_even_with_progress() {
        for frozen in [true, false] {
            let (plane, mut module) = rig();
            let ctx = batch_ctx("declared-progress");
            let call = call("read");
            let manifest = if frozen {
                let plan = frozen_manifest(false, Some(120_000));
                ToolManifest::from_tools(plan.tools.as_ref().clone())
                    .with_plan_manifest(&plan)
                    .unwrap()
            } else {
                ToolManifest::from_tools(vec![serde_json::from_value(serde_json::json!({
                    "name":"read", "description":"probe", "input_schema":{}, "module":"aft",
                    "reply":{"max_ms":120_000}
                }))
                .unwrap()])
            };
            let dispatch = async {
                plane
                    .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
                    .await
                    .unwrap();
                plane.dispatch(&ctx, &call).await
            };
            let provider = async {
                let (request, keepalive) = bound_request(&mut module).await;
                assert!(!keepalive);
                let started = Instant::now();
                for _ in 0..3 {
                    sleep(Duration::from_secs(30)).await;
                    assert!(module.push(
                        &request,
                        br#"{"kind":"progress","message":"working"}"#.to_vec()
                    ));
                }
                (request, started)
            };
            let (outcome, (request, started)) = tokio::join!(dispatch, provider);
            assert_eq!(
                started.elapsed(),
                Duration::from_secs(120),
                "frozen={frozen}"
            );
            assert!(
                matches!(outcome, DispatchResult::SentButOutcomeUnknown { ref reason }
                if reason.contains("timed out after 120s")),
                "{outcome:?}"
            );
            assert_cancel_and_ignore_late(&mut module, &request);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn run_cancel_cancels_every_outstanding_request() {
        for keepalive in [false, true] {
            let (plane, mut module) = rig();
            let signal = CancellationToken::new();
            let control = RunControl {
                cancel: Some(signal.clone()),
                ..RunControl::default()
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
