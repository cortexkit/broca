use super::*;
use broca_wal::{FetchOutcome, OpaqueJson, PlanManifest, ProviderFetch};
use serde_json::json;

pub(in crate::subc_plane) fn catalog(tagged: bool, pinned: bool) -> serde_json::Value {
    let mut tools = Vec::new();
    for name in ["bash", "bash_watch"] {
        let mut tool = json!({"name":name,"description":"Run work", "input_schema":{"type":"object"},
            "capabilities":if tagged { vec!["call.yield/v1"] } else { vec![] }, "result_format":"tool-result/v1"});
        if pinned {
            tool["schema_digest"] = json!("a".repeat(64));
            tool["semantics"] = json!(2);
        }
        tools.push(tool);
    }
    json!({"generation":"one","catalog_digest":"one","tools":tools,"capabilities":{"late_results":true}})
}

pub(in crate::subc_plane) fn plan(provider: &str, pinned: bool) -> PlanManifest {
    let (catalog, declarations) = broca_fetch_plan::admit::decode_transport_catalog(
        provider,
        catalog(false, pinned),
        true,
        true,
    )
    .unwrap();
    PlanManifest {
        rich_ui: None,
        plan: OpaqueJson::from_json_text(
            json!({"tool_items":[{"provider":provider,"preset":"worker"}]}).to_string(),
        )
        .unwrap(),
        plan_identity: "identity".into(),
        composition_digest: "composition".into(),
        fetches: vec![ProviderFetch {
            provider: provider.into(),
            outcome: FetchOutcome::Answered {
                catalog_digest: "original".into(),
                tools: catalog.tools.clone().into(),
                text: None,
                result_ops: None,
                reply: None,
                declarations_frozen: Some(Box::new(declarations)),
                reference_slots: None,
            },
        }],
        tools: catalog.tools.into(),
        system_text: "Frozen system".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: Vec::new(),
    }
}

pub(in crate::subc_plane) fn inventory(provider: &str) -> Vec<CatalogEntry> {
    let mut entry = CatalogEntry::new(provider, Vec::new(), Vec::new()).with_ready(true);
    entry.capabilities = Some(
        serde_json::from_value(
            json!({"provides":["tool-provider/v1","call-keepalive/v1","call-yield/v1"]}),
        )
        .unwrap(),
    );
    vec![entry]
}

#[tokio::test(start_paused = true)]
async fn run_transport_cache_expires_after_a_minute_and_route_invalidation_forces_a_read() {
    let provider = "transport-ttl";
    let plan = plan(provider, true);
    let conn = ScriptedToolPlaneConnection::new(
        vec![inventory(provider); 3],
        vec![
            Ok(catalog(false, true)),
            Ok(catalog(true, true)),
            Ok(catalog(false, true)),
        ],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), provider);
    let bind = BindIdentity::new("/project", "test", "ttl");
    assert!(plane.run_transport(&bind, None, &plan).await.is_empty());
    tokio::time::advance(Duration::from_secs(59)).await;
    let another_plane = SubcToolPlane::with_connection(conn.clone(), provider);
    assert!(another_plane
        .run_transport(&bind, None, &plan)
        .await
        .is_empty());
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        conn.dispatched_presets.lock().unwrap()[0].as_deref(),
        Some("worker")
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    let changed = plane.run_transport(&bind, None, &plan).await;
    assert_eq!(
        changed[provider].yield_tools.as_ref().unwrap(),
        &BTreeSet::from(["bash".into(), "bash_watch".into()])
    );
    assert!(changed[provider].schema_pins.is_none());
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 2);
    plane.invalidate_route(&bind);
    assert!(plane.run_transport(&bind, None, &plan).await.is_empty());
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn run_transport_malformed_catalog_keeps_the_manifest_and_warns_once() {
    let provider = "transport-malformed";
    let plan = plan(provider, true);
    let conn = ScriptedToolPlaneConnection::new(vec![inventory(provider)], vec![Ok(json!(7))]);
    let plane = SubcToolPlane::with_connection(conn.clone(), provider);
    let bind = BindIdentity::new("/project", "test", provider);
    let capture = broca_test_support::LogCapture::new();
    for _ in 0..2 {
        assert!(capture
            .scope_future(plane.run_transport(&bind, None, &plan))
            .await
            .is_empty());
    }
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        capture
            .events()
            .iter()
            .filter(|event| event.target == "tool_transport")
            .count(),
        1
    );
}

#[tokio::test]
async fn run_transport_successfully_withdrawn_promises_override_but_never_replace_pins() {
    let provider = "transport-withdrawn";
    let plan = plan(provider, true);
    let mut answer = catalog(false, true);
    answer["capabilities"] = json!({"late_results":false});
    for tool in answer["tools"].as_array_mut().unwrap() {
        tool.as_object_mut().unwrap().remove("result_format");
    }
    let conn = ScriptedToolPlaneConnection::new(
        vec![vec![
            CatalogEntry::new(provider, Vec::new(), Vec::new()).with_ready(true)
        ]],
        vec![Ok(answer)],
    );
    let plane = SubcToolPlane::with_connection(conn, provider);
    let bind = BindIdentity::new("/project", "test", provider);
    let facts = plane.run_transport(&bind, None, &plan).await;
    assert_eq!(
        facts[provider],
        broca_wal::FrozenProviderDeclarations::default()
    );
    let dispatch = ToolManifest::from_shared_tools(Arc::clone(&plan.tools))
        .with_plan_manifest(&plan)
        .unwrap();
    let pins = dispatch
        .provider_declarations(provider)
        .unwrap()
        .schema_pins
        .clone();
    let updated = dispatch.with_run_transport(&facts);
    let effective = updated.provider_declarations(provider).unwrap();
    assert!(effective.call_keepalive.is_none());
    assert!(effective.late_results.is_none());
    assert!(effective.result_formats.is_none());
    assert!(effective.yield_tools.is_none());
    assert_eq!(effective.schema_pins, pins);
}

#[tokio::test(start_paused = true)]
async fn run_transport_failed_or_slow_reads_keep_stale_facts_and_warn_once_per_cause() {
    for stale in [false, true] {
        let provider = if stale {
            "transport-stale"
        } else {
            "transport-empty"
        };
        let plan = plan(provider, true);
        let conn = ScriptedToolPlaneConnection::new(
            vec![inventory(provider); 5],
            if stale {
                vec![
                    Ok(catalog(true, true)),
                    Err(ConnError::Protocol("private failure".into())),
                    Err(ConnError::Protocol("private failure".into())),
                ]
            } else {
                vec![
                    Err(ConnError::Protocol("private failure".into())),
                    Err(ConnError::Protocol("private failure".into())),
                ]
            },
        );
        let plane = SubcToolPlane::with_connection(conn.clone(), provider);
        let bind = BindIdentity::new("/project", "test", provider);
        if stale {
            assert!(!plane.run_transport(&bind, None, &plan).await.is_empty());
        }
        tokio::time::advance(Duration::from_secs(60)).await;
        let capture = broca_test_support::LogCapture::new();
        for _ in 0..2 {
            let facts = capture
                .scope_future(plane.run_transport(&bind, None, &plan))
                .await;
            assert_eq!(!facts.is_empty(), stale);
        }
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(60)).await;
            conn.script_route_open(provider, Duration::from_secs(10), None);
            let facts = capture
                .scope_future(plane.run_transport(&bind, None, &plan))
                .await;
            assert_eq!(!facts.is_empty(), stale);
        }
        let events = capture.events();
        let warnings: Vec<_> = events
            .iter()
            .filter(|event| event.target == "tool_transport")
            .collect();
        assert_eq!(
            warnings.len(),
            2,
            "one failure and one timeout warning: {warnings:?}"
        );
        assert!(warnings
            .iter()
            .all(|event| !format!("{event:?}").contains("private failure")));
    }
}

#[tokio::test(start_paused = true)]
async fn run_transport_failed_and_slow_attempts_are_throttled_until_invalidation_or_a_minute() {
    for slow in [false, true] {
        let provider = if slow {
            "transport-throttle-slow"
        } else {
            "transport-throttle-failed"
        };
        let plan = plan(provider, true);
        let replies = if slow {
            vec![Ok(catalog(true, true)), Ok(catalog(false, true))]
        } else {
            vec![
                Err(ConnError::Protocol("unavailable".into())),
                Ok(catalog(true, true)),
                Ok(catalog(false, true)),
            ]
        };
        let conn = ScriptedToolPlaneConnection::new(vec![inventory(provider); 3], replies);
        if slow {
            conn.script_route_open(provider, Duration::from_secs(10), None);
        }
        let plane = SubcToolPlane::with_connection(conn.clone(), provider);
        let bind = BindIdentity::new("/project", "test", provider);
        let began = Instant::now();
        assert!(plane.run_transport(&bind, None, &plan).await.is_empty());
        assert!(plane.run_transport(&bind, None, &plan).await.is_empty());
        tokio::time::advance(Duration::from_secs(59).saturating_sub(began.elapsed())).await;
        assert!(plane.run_transport(&bind, None, &plan).await.is_empty());
        assert_eq!(
            conn.catalog_calls.load(Ordering::SeqCst),
            1,
            "failed reads are not retried by intervening runs"
        );
        conn.script_route_open(provider, Duration::ZERO, None);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(!plane.run_transport(&bind, None, &plan).await.is_empty());
        assert_eq!(conn.catalog_calls.load(Ordering::SeqCst), 2);
        plane.invalidate_route(&bind);
        assert!(plane.run_transport(&bind, None, &plan).await.is_empty());
        assert_eq!(
            conn.catalog_calls.load(Ordering::SeqCst),
            3,
            "invalidation clears the attempt throttle too"
        );
    }
}

#[tokio::test]
async fn run_transport_schema_drift_freezes_the_whole_provider_with_definition_fallback() {
    for (suffix, pinned, drift) in [
        ("pins", true, true),
        ("definition", false, true),
        ("unchanged", false, false),
        ("missing", true, true),
    ] {
        let provider = format!("transport-schema-{suffix}");
        let plan = plan(&provider, pinned);
        let original = serde_json::to_vec(&plan).unwrap();
        let mut answer = catalog(true, pinned);
        if suffix == "pins" {
            answer["tools"][0]["schema_digest"] = json!("b".repeat(64));
        }
        if suffix == "definition" {
            answer["tools"][0]["description"] = json!("Changed");
        }
        if suffix == "missing" {
            answer["tools"].as_array_mut().unwrap().remove(0);
        }
        let conn = ScriptedToolPlaneConnection::new(vec![inventory(&provider)], vec![Ok(answer)]);
        let plane = SubcToolPlane::with_connection(conn, &provider);
        let bind = BindIdentity::new("/project", "test", &provider);
        let (facts, logs) =
            broca_test_support::capture_logs(plane.run_transport(&bind, None, &plan)).await;
        assert_eq!(facts.is_empty(), drift);
        assert_eq!(
            logs.iter()
                .filter(|event| event.target == "tool_transport")
                .count(),
            usize::from(drift)
        );
        assert_eq!(
            serde_json::to_vec(&plan).unwrap(),
            original,
            "manifest bytes never change"
        );
    }
}
