use super::*;
use broca_core::{RunIndex, RunIndexAttribution, RunIndexRow, RunIndexState};
use broca_protocol::{BindIdentity, BindScope, SessionScope};
use std::cell::Cell;

const NOW: i64 = 200_000_000;
fn principal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}
fn scope(owner: BindPrincipal, kind: ScopeKind, flow: bool, authorized: bool) -> SendScope {
    SendScope::Stamped(BindScope {
        scope: SessionScope {
            owner,
            scope_ref: "head".into(),
            scope_epoch: 1,
            flow_id: flow.then(|| "flow".into()),
            sender: flow.then(principal),
            agent_id: None,
            run_id: None,
        },
        kind,
        parent: None,
        parent_state: None,
        owner_authorized: authorized,
    })
}
fn healthy(store: &broca_store::SqliteStore, since: i64) -> Evidence {
    Evidence {
        runs: store.diagnostic_evidence(since, NOW).unwrap(),
        late_fence_ms: None,
        retention: Ok(vec![]),
        catalog_age_ms: Ok(100),
        unmapped_credentials: BTreeMap::new(),
        rings: ["send_refusals", "catalog_stale", "tool_outages"]
            .map(|check| {
                (
                    check.into(),
                    Ok(Snapshot {
                        since_ms: NOW - 1000,
                        truncated: false,
                        events: vec![],
                    }),
                )
            })
            .into(),
    }
}
type SeedDetails<'a> = (
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);
fn seed(
    store: &broca_store::SqliteStore,
    id: &str,
    state: RunIndexState,
    at: i64,
    details: SeedDetails<'_>,
) {
    let (provider, class, code, pause, interrupt) = details;
    seed_active(store, id);
    store
        .index_run_attributed(
            id,
            state,
            None,
            None,
            0,
            &RunIndexAttribution {
                provider_module_id: provider.map(str::to_owned),
                credential_id: Some("credential".into()),
                error_class: class.map(str::to_owned),
                error_code: code.map(str::to_owned),
                pause_reason: pause.map(str::to_owned),
                interrupt_reason: interrupt.map(str::to_owned),
                at_ms: Some(at as u64),
            },
        )
        .unwrap();
}
fn seed_active(store: &broca_store::SqliteStore, id: &str) {
    store
        .index_run_active(&RunIndexRow {
            run_id: id.into(),
            session: BindIdentity::new("/project", "test", "session"),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
            title: None,
            withdrawals_owed: false,
        })
        .unwrap();
}
fn seed_legacy(store: &broca_store::SqliteStore, id: &str, state: RunIndexState) {
    seed_active(store, id);
    store
        .index_run_attributed(id, state, None, None, 0, &RunIndexAttribution::default())
        .unwrap();
}

fn metadata_scope_listing() -> broca_subc::vault::ScopedList {
    let credentials: Vec<_> = [
        ("apikey:cline-pass", Some("apikey"), None),
        ("apikey:muse", Some("apikey"), None),
        ("antigravity-work", Some("antigravity"), Some("google")),
        ("oauth:cursor", Some("oauth"), Some("cursor")),
        ("opaque-claude", Some("oauth"), Some("anthropic")),
        ("opaque-chatgpt", Some("chatgpt"), Some("openai")),
        ("no-adapter", Some("oauth"), None),
        ("unknown-adapter", Some("oauth"), Some("future-adapter")),
        ("unknown-method", Some("future-method"), None),
        ("missing-method", None, None),
    ]
    .map(|(id, method, adapter)| {
        json!({
            "id":id,"categories":["llm-provider"],"type":"oauth","serves":[],
            "state":"active","record_version":1,"operations":["read"],
            "auth_method":method,"refresh_adapter":adapter
        })
    })
    .into();
    serde_json::from_value(json!({
        "credentials":credentials,"grants":0,"grant_tuples":[],"view":"test"
    }))
    .unwrap()
}
fn unmapped_credentials() -> BTreeMap<String, &'static str> {
    let unmapped = crate::account_routing::UnmappedCredentials::default();
    unmapped.observe(&metadata_scope_listing());
    unmapped.snapshot()
}

#[test]
fn diagnostics_unmapped_credentials_use_chooser_scope_and_name_vault_fixes() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    let mut evidence = healthy(&store, 0);
    evidence.unmapped_credentials = unmapped_credentials();
    let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
    let keys: Vec<_> = reply.findings.iter().map(|f| f.key.as_str()).collect();
    assert_eq!(
        keys,
        [
            "missing-method",
            "no-adapter",
            "opaque-chatgpt",
            "opaque-claude",
            "unknown-adapter",
            "unknown-method"
        ]
    );
    for f in &reply.findings {
        assert_eq!(f.check, "credential_metadata_missing");
        assert_eq!(f.severity, Severity::Warn);
        assert_eq!(f.count, 1);
        assert_eq!(f.samples[0]["credential_id"], f.key);
        assert!(f.hint.contains("vault"));
        assert!(f.hint.contains("non-model credential type"));
    }
}

#[test]
fn diagnostics_legacy_null_rows_are_one_info_finding_not_operational_errors() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    for (id, state) in [
        ("legacy-pause", RunIndexState::Paused),
        ("legacy-error", RunIndexState::Error),
        ("legacy-interruption", RunIndexState::Interrupted),
    ] {
        seed_legacy(&store, id, state);
    }
    let reply = answer(
        json!({}),
        Some(&principal()),
        &scope(principal(), ScopeKind::Head, false, true),
        NOW,
        |since| Ok(healthy(&store, since)),
    )
    .unwrap();
    assert_eq!(reply.findings.len(), 1, "{:#?}", reply.findings);
    let finding = &reply.findings[0];
    assert_eq!(finding.check, "data_quality");
    assert_eq!(finding.severity, Severity::Info);
    assert_eq!(finding.count, 3);
    assert!(finding.samples[0]["oldest_row_ms"].is_null());
    assert!(finding.samples[0]["newest_row_ms"].is_null());
    assert_eq!(
        finding.samples[0]["counts_by_check"],
        json!({"paused_runs":1,"run_errors":1,"interrupted_runs":1})
    );
    for field in [
        "state_changed_ms",
        "provider_module_id",
        "credential_id",
        "error_class",
        "error_code",
        "pause_reason",
        "interrupt_reason",
    ] {
        assert_eq!(finding.samples[0]["missing_fields"][field], 3, "{field}");
    }
}

#[test]
fn diagnostics_auth_pauses_keep_severity_alongside_legacy_rows() {
    for (age, severity) in [(100, Severity::Warn), (90_000_000, Severity::Error)] {
        let store = broca_store::SqliteStore::open_in_memory().unwrap();
        seed_legacy(&store, "legacy-pause", RunIndexState::Paused);
        seed(
            &store,
            "auth-pause",
            RunIndexState::Paused,
            NOW - age,
            (Some("anthropic"), None, None, Some("auth_required"), None),
        );
        seed(
            &store,
            "other-pause",
            RunIndexState::Paused,
            NOW - 100,
            (None, None, None, Some("waiting"), None),
        );
        let reply = report(healthy(&store, 0), NOW, DEFAULT_WINDOW_MS);
        assert_eq!(reply.findings.len(), 2);
        let pause = reply
            .findings
            .iter()
            .find(|f| f.check == "paused_runs")
            .unwrap();
        assert_eq!(pause.count, 1);
        assert_eq!(pause.severity, severity);
        assert_eq!(pause.samples[0]["run_id"], "auth-pause");
    }
}

#[test]
fn diagnostics_partial_attribution_reports_known_row_time_bounds() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    seed(
        &store,
        "partial-error",
        RunIndexState::Error,
        NOW - 200,
        (None, Some("auth_required"), None, None, None),
    );
    seed(
        &store,
        "partial-interruption",
        RunIndexState::Interrupted,
        NOW - 100,
        (Some("anthropic"), None, None, None, None),
    );
    seed_legacy(&store, "untimed", RunIndexState::Error);
    let reply = report(healthy(&store, 0), NOW, DEFAULT_WINDOW_MS);
    assert_eq!(reply.findings.len(), 1);
    let quality = &reply.findings[0];
    assert_eq!(quality.severity, Severity::Info);
    assert_eq!(quality.count, 3);
    assert_eq!(quality.samples[0]["oldest_row_ms"], NOW - 200);
    assert_eq!(quality.samples[0]["newest_row_ms"], NOW - 100);
    assert_eq!(quality.samples[0]["missing_fields"]["state_changed_ms"], 1);
}

#[test]
fn diagnostics_admission_never_reads_refused_routes() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    let mut flow_with_owner_run = scope(principal(), ScopeKind::Head, true, true);
    if let SendScope::Stamped(stamp) = &mut flow_with_owner_run {
        stamp.scope.run_id = Some("owners-run".into());
        stamp.scope.agent_id = Some("owner-agent".into());
    }
    for (caller, stamp, admitted) in [
        (principal(), flow_with_owner_run, true),
        (
            principal(),
            scope(principal(), ScopeKind::Head, false, true),
            true,
        ),
        (
            principal(),
            scope(principal(), ScopeKind::Head, true, true),
            true,
        ),
        (
            BindPrincipal::Direct,
            scope(BindPrincipal::Direct, ScopeKind::Head, false, true),
            true,
        ),
        (BindPrincipal::Direct, SendScope::Unstamped, false),
        (
            BindPrincipal::Direct,
            scope(principal(), ScopeKind::Head, true, true),
            false,
        ),
        (
            BindPrincipal::Unverified,
            scope(principal(), ScopeKind::Head, true, true),
            false,
        ),
        (
            principal(),
            scope(principal(), ScopeKind::Worker, true, true),
            false,
        ),
        (
            BindPrincipal::Direct,
            scope(principal(), ScopeKind::Head, false, true),
            false,
        ),
        (
            principal(),
            scope(principal(), ScopeKind::Worker, false, true),
            false,
        ),
        (
            principal(),
            scope(principal(), ScopeKind::Head, true, false),
            false,
        ),
        (principal(), SendScope::DaemonWithoutScopes, false),
    ] {
        let reads = Cell::new(0);
        let reply = answer(json!({}), Some(&caller), &stamp, NOW, |since| {
            reads.set(reads.get() + 1);
            Ok(healthy(&store, since))
        });
        assert_eq!(reply.is_ok(), admitted);
        assert_eq!(reads.get(), usize::from(admitted));
        if let Err(error) = reply {
            assert_eq!(error.code, "permission_denied");
        }
    }
}

#[test]
fn diagnostics_window_validation_and_unknown_params() {
    use broca_wire::diagnostics::MAX_WINDOW_MS;
    for (value, expected) in [
        (Value::Null, Some(DEFAULT_WINDOW_MS)),
        (json!({}), Some(DEFAULT_WINDOW_MS)),
        (json!({"window_ms":null}), Some(DEFAULT_WINDOW_MS)),
        (json!({"window_ms":0}), None),
        (json!({"window_ms":1}), Some(1)),
        (json!({"window_ms":MAX_WINDOW_MS}), Some(MAX_WINDOW_MS)),
        (json!({"window_ms":MAX_WINDOW_MS+1}), None),
        (json!({"window_ms":-1}), None),
        (json!({"window_ms":1.5}), None),
        (json!({"window_ms":"1"}), None),
        (json!({"extra":true}), None),
        (json!([]), None),
    ] {
        assert_eq!(Request::window(value).ok(), expected);
    }
}

#[test]
fn diagnostics_seeded_checks_floor_and_restart_exclusion() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    seed(
        &store,
        "paused",
        RunIndexState::Paused,
        NOW - 90_000_000,
        (Some("openai"), None, None, Some("auth_required"), None),
    );
    seed(
        &store,
        "billing",
        RunIndexState::Error,
        NOW - 100,
        (
            Some("openai"),
            Some("permanent"),
            Some("billing_exhausted"),
            None,
            None,
        ),
    );
    seed(
        &store,
        "scattered",
        RunIndexState::Error,
        NOW - 90,
        (
            Some("openai"),
            Some("permanent"),
            Some("cyber_policy"),
            None,
            None,
        ),
    );
    seed(
        &store,
        "unknown",
        RunIndexState::Error,
        NOW - 80,
        (None, None, None, None, None),
    );
    seed(
        &store,
        "interrupted",
        RunIndexState::Interrupted,
        NOW - 70,
        (Some("openai"), None, None, None, None),
    );
    seed(
        &store,
        "shutdown",
        RunIndexState::Interrupted,
        NOW - 60,
        (Some("openai"), None, None, None, Some("runner_shutdown")),
    );
    seed(
        &store,
        "crash",
        RunIndexState::Interrupted,
        NOW - 50,
        (Some("openai"), None, None, None, Some("runner_crash")),
    );
    seed(
        &store,
        "transform",
        RunIndexState::TransformUnavailable,
        NOW - 40,
        (Some("openai"), None, None, None, None),
    );
    seed(
        &store,
        "outside",
        RunIndexState::Error,
        NOW - DEFAULT_WINDOW_MS - 1,
        (Some("openai"), Some("auth_required"), None, None, None),
    );
    let mut evidence = healthy(&store, NOW - DEFAULT_WINDOW_MS);
    evidence.late_fence_ms = Some(NOW - 200);
    evidence.retention = Ok(vec![Fence {
        session: "address".into(),
        kind: "provider_held_intake_unknown".into(),
        first_ms: NOW - 200,
        last_ms: NOW - 100,
    }]);
    evidence.catalog_age_ms = Ok(22_000_000);
    evidence.unmapped_credentials = [
        ("claude-work".into(), "provider_ids"),
        ("chatgpt-work".into(), "auth_method"),
    ]
    .into();
    for (check, module, code) in [
        ("send_refusals", "routing", "account_limit_exhausted"),
        ("tool_outages", "aft", "module_unavailable"),
    ] {
        evidence
            .rings
            .get_mut(check)
            .unwrap()
            .as_mut()
            .unwrap()
            .events
            .push(Event {
                at_ms: NOW - 50,
                module: module.into(),
                code: code.into(),
            });
    }
    let reply = answer(
        json!({}),
        Some(&principal()),
        &scope(principal(), ScopeKind::Head, false, true),
        NOW,
        |_| Ok(evidence),
    )
    .unwrap();
    for check in [
        "paused_runs",
        "deletion_fenced",
        "late_results_rebuild_required",
        "catalog_stale",
        "credential_metadata_missing",
        "send_refusals",
        "run_errors",
        "tool_outages",
        "interrupted_runs",
    ] {
        assert!(
            reply.findings.iter().any(|f| f.check == check),
            "missing {check}"
        );
    }
    assert_eq!(
        reply
            .findings
            .iter()
            .filter(|f| f.check == "run_errors")
            .map(|f| f.count)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        reply
            .findings
            .iter()
            .filter(|f| f.check == "interrupted_runs")
            .map(|f| f.count)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        reply
            .findings
            .iter()
            .find(|f| f.check == "data_quality")
            .unwrap()
            .count,
        2
    );
    assert_eq!(
        reply
            .findings
            .iter()
            .find(|f| f.check == "paused_runs")
            .unwrap()
            .severity,
        Severity::Error
    );
    assert!(reply
        .findings
        .windows(2)
        .all(|pair| pair[0].severity >= pair[1].severity));
    assert_eq!(
        error_floor("permanent", "request_invalid", DEFAULT_WINDOW_MS),
        (10, Severity::Error)
    );
    assert_eq!(
        error_floor("context_overflow", "", DEFAULT_WINDOW_MS),
        (25, Severity::Warn)
    );
    assert_eq!(
        error_floor("transient", "", DEFAULT_WINDOW_MS),
        (50, Severity::Warn)
    );
    assert_eq!(error_floor("permanent", "request_invalid", 1).0, 3);
    assert_eq!(
        error_floor("permanent", "request_invalid", DEFAULT_WINDOW_MS * 2).0,
        20
    );
}

#[test]
fn a_catalog_aged_just_past_one_poll_interval_is_not_stale() {
    // A healthy poller refreshes one interval after its last refresh finished, so the catalog
    // is briefly older than one interval before every refresh. Only a missed poll is stale.
    let interval = crate::CATALOG_POLL_INTERVAL.as_millis() as i64;
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    for (age, stale) in [(interval + 5_000, false), (2 * interval + 1, true)] {
        let mut evidence = healthy(&store, NOW - DEFAULT_WINDOW_MS);
        evidence.catalog_age_ms = Ok(age);
        let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
        let found = reply.findings.iter().any(|f| f.check == "catalog_stale");
        assert_eq!(found, stale, "catalog aged {age} ms");
    }
}

#[test]
fn diagnostics_unreadable_sources_are_not_silent() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    seed_legacy(&store, "legacy", RunIndexState::Error);
    let mut evidence = healthy(&store, 0);
    evidence.retention = Err("unreadable".into());
    evidence.catalog_age_ms = Err("unreadable".into());
    for value in evidence.rings.values_mut() {
        *value = Err("unreadable".into());
    }
    let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
    for check in [
        "deletion_fenced",
        "catalog_stale",
        "send_refusals",
        "tool_outages",
    ] {
        assert!(reply.findings.iter().any(|f| f.check == check
            && f.key == "evidence_unavailable"
            && f.severity == Severity::Error));
    }
    assert!(reply
        .findings
        .iter()
        .any(|f| f.check == "data_quality" && f.severity == Severity::Info));
    let root = broca_test_support::scratch_root().join("diagnostics-unreadable-source");
    let result = answer(
        json!({}),
        Some(&principal()),
        &scope(principal(), ScopeKind::Head, false, true),
        NOW,
        |since| gather(&root, Ok(0), since, NOW),
    );
    assert_eq!(result.unwrap_err().code, "store_error");
}

#[test]
fn diagnostics_trimming_keeps_severity_counts_and_sample_cap_under_64k() {
    let findings = (0..500)
        .map(|i| {
            let mut f = finding(
                "test",
                format!("key-{i:04}"),
                if i % 2 == 0 {
                    Severity::Warn
                } else {
                    Severity::Error
                },
                1,
                2,
                &"summary".repeat(50),
                "Look at health.check.",
            );
            f.count = 900;
            f.samples = vec![json!({"id":"x".repeat(200)}); 10];
            f
        })
        .collect();
    let reply = trim(Reply {
        generated_at_ms: NOW,
        window_ms: DEFAULT_WINDOW_MS,
        findings,
        coverage: BTreeMap::new(),
    });
    assert!(!reply.findings.is_empty() && reply.findings.len() < 500);
    assert!(serde_json::to_vec(&json!({"result":reply})).unwrap().len() < REPLY_LIMIT);
    assert!(reply
        .findings
        .iter()
        .all(|f| f.count == 900 && f.samples.len() == 5 && f.severity == Severity::Error));
}

fn vector(name: &str, value: Value) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/vectors/diagnostics-findings-v1")
        .join(name);
    let bytes = format!("{}\n", serde_json::to_string_pretty(&value).unwrap());
    if std::env::var_os("UPDATE_BROCA_DIAGNOSTICS_FINDINGS_VECTORS").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        println!("VECTOR {name}\n{bytes}END VECTOR");
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), bytes);
}

#[test]
fn diagnostics_findings_v1_vectors_match_serving_replies() {
    let root = broca_test_support::scratch_root()
        .join(format!("diagnostics-vectors-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let store = broca_store::SqliteStore::open(root.join("run-index.db")).unwrap();
    for (name, params) in [
        ("request-empty.json", json!({})),
        ("request-window.json", json!({"window_ms":60000})),
        (
            "request-max-window.json",
            json!({"window_ms":broca_wire::diagnostics::MAX_WINDOW_MS}),
        ),
    ] {
        vector(name, json!({"method":OP,"params":params}));
    }
    vector(
        "credential-scope.json",
        json!({
            "listing":metadata_scope_listing(),
            "flagged":unmapped_credentials().keys().collect::<Vec<_>>()
        }),
    );
    let head = scope(principal(), ScopeKind::Head, false, true);
    let reply = answer(json!({}), Some(&principal()), &head, NOW, |since| {
        Ok(healthy(&store, since))
    })
    .unwrap();
    assert!(reply.findings.is_empty());
    assert_eq!(reply.coverage["send_refusals"].since_ms, NOW - 1000);
    vector("reply-healthy.json", json!({"result":reply}));
    seed(
        &store,
        "paused",
        RunIndexState::Paused,
        NOW - 90_000_000,
        (
            Some("openai"),
            Some("auth_required"),
            None,
            Some("auth_required"),
            None,
        ),
    );
    seed(
        &store,
        "billing",
        RunIndexState::Error,
        NOW - 100,
        (
            Some("openai"),
            Some("permanent"),
            Some("billing_exhausted"),
            None,
            None,
        ),
    );
    seed_legacy(&store, "legacy-pause", RunIndexState::Paused);
    seed_legacy(&store, "legacy-error", RunIndexState::Error);
    seed_legacy(&store, "legacy-interruption", RunIndexState::Interrupted);
    seed(
        &store,
        "partial-error",
        RunIndexState::Error,
        NOW - 200,
        (None, Some("auth_required"), None, None, None),
    );
    let reply = answer(json!({}), Some(&principal()), &head, NOW, |since| {
        let mut evidence = healthy(&store, since);
        evidence.unmapped_credentials = unmapped_credentials();
        Ok(evidence)
    })
    .unwrap();
    vector("reply-findings.json", json!({"result":reply}));
}

#[test]
fn diagnostics_error_floors_require_bursts_but_auth_requires_one() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    for (class, code, total) in [
        ("permanent", "request_invalid", 9),
        ("transient", "rate_limited", 49),
        ("context_overflow", "context_length_exceeded", 24),
        ("auth_required", "invalid_api_key", 1),
    ] {
        for n in 0..total {
            seed(
                &store,
                &format!("{class}-{n}"),
                RunIndexState::Error,
                NOW - 100,
                (Some("provider"), Some(class), Some(code), None, None),
            );
        }
    }
    let before = report(
        healthy(&store, NOW - DEFAULT_WINDOW_MS),
        NOW,
        DEFAULT_WINDOW_MS,
    );
    assert_eq!(
        before
            .findings
            .iter()
            .filter(|f| f.check == "run_errors")
            .count(),
        1
    );
    for (class, code, n) in [
        ("permanent", "request_invalid", 9),
        ("transient", "rate_limited", 49),
        ("context_overflow", "context_length_exceeded", 24),
    ] {
        seed(
            &store,
            &format!("{class}-{n}"),
            RunIndexState::Error,
            NOW - 100,
            (Some("provider"), Some(class), Some(code), None, None),
        );
    }
    let after = report(
        healthy(&store, NOW - DEFAULT_WINDOW_MS),
        NOW,
        DEFAULT_WINDOW_MS,
    );
    assert_eq!(
        after
            .findings
            .iter()
            .filter(|f| f.check == "run_errors")
            .count(),
        4
    );
    assert_eq!(
        after
            .findings
            .iter()
            .filter(|f| f.severity == Severity::Error)
            .count(),
        2
    );
}

#[test]
fn diagnostics_reads_rebuild_marker_without_session_access() {
    let root = broca_test_support::scratch_root()
        .join(format!("diagnostics-marker-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let _store = broca_store::SqliteStore::open(root.join("run-index.db")).unwrap();
    std::fs::write(root.join("late-results-rebuild-required"), b"").unwrap();
    let evidence = gather(&root, Ok(0), 0, live::now_ms()).unwrap();
    let reply = report(evidence, live::now_ms(), DEFAULT_WINDOW_MS);
    assert!(reply
        .findings
        .iter()
        .any(|f| f.check == "late_results_rebuild_required" && f.severity == Severity::Error));
}

#[test]
fn diagnostics_untimed_overflow_is_explicit_in_finding_and_coverage() {
    let store = broca_store::SqliteStore::open_in_memory().unwrap();
    let mut evidence = healthy(&store, 0);
    evidence.runs.untimed_errors = vec![
        RunEvidence {
            run_id: "legacy".into(),
            state: "error".into(),
            at_ms: None,
            provider: None,
            credential: None,
            class: None,
            code: None,
            pause_reason: None,
            interrupt_reason: None,
        };
        SCAN_LIMIT + 1
    ];
    let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
    let finding = reply
        .findings
        .iter()
        .find(|f| f.check == "data_quality")
        .unwrap();
    assert_eq!(finding.count, SCAN_LIMIT as u64);
    assert_eq!(finding.severity, Severity::Info);
    assert_eq!(
        finding.samples[0]["truncated_checks"],
        json!(["run_errors"])
    );
    assert!(reply.coverage["run_errors"].truncated);
    assert_eq!(finding.samples[0]["scan_limit"], 20000);
}
