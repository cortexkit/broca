use super::*;
use broca_protocol::{BindScope, PauseReason, SessionScope};
use std::cell::Cell;

const NOW: i64 = 200_000_000;

#[test]
fn diagnostics_address_unavailability_is_an_error_from_the_first_refusal_per_reason() {
    let mut evidence = healthy();
    evidence.rings.insert(
        "session_address_unavailable".into(),
        Ok(Snapshot {
            since_ms: NOW - 1000,
            truncated: false,
            events: [
                "incomplete_inventory",
                "gap_marker",
                "store_error",
                "wait_expired",
                "migration_failed",
            ]
            .into_iter()
            .map(|code| Event {
                at_ms: NOW - 1,
                module: String::new(),
                code: code.into(),
            })
            .collect(),
        }),
    );
    let reply = report(evidence, NOW, 1000);
    let findings: Vec<_> = reply
        .findings
        .iter()
        .filter(|finding| finding.check == "session_address_unavailable")
        .collect();
    assert_eq!(findings.len(), 5);
    for (reason, words) in [
        ("incomplete_inventory", "incomplete inventory"),
        ("gap_marker", "gap marker"),
        ("store_error", "store error"),
        ("wait_expired", "wait expired"),
        ("migration_failed", "migration failed"),
    ] {
        let finding = findings
            .iter()
            .find(|finding| finding.key == reason)
            .unwrap();
        assert_eq!(finding.severity, Severity::Error);
        assert_eq!(finding.count, 1);
        assert_eq!(finding.first_seen_ms, NOW - 1);
        assert!(finding.summary.contains(words));
    }
}

fn principal() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}
fn scope(owner: BindPrincipal, kind: ScopeKind, flow: bool, authorized: bool) -> SendScope {
    SendScope::Stamped(BindScope {
        scope: SessionScope {
            owner,
            agent_id_frozen: false,
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
fn healthy() -> Evidence {
    Evidence {
        tool_result_json_dumps: Ok(broca_subc::tool_result_diagnostics::Snapshot {
            since_ms: NOW - 1000,
            truncated: false,
            tools: vec![],
        }),
        runs: Ok(RunEvidence {
            window: Snapshot {
                since_ms: NOW - 1000,
                truncated: false,
                events: vec![],
            },
            held: vec![],
        }),
        late_fence_ms: None,
        late_delivery_stuck: vec![],
        retention: Ok(vec![]),
        catalog_age_ms: Ok(100),
        unmapped_credentials: BTreeMap::new(),
        rings: [
            "send_refusals",
            "catalog_stale",
            "tool_outages",
            "session_address_unavailable",
        ]
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

impl Evidence {
    /// The evidence with its run outcomes `outcomes`: those from `since` in the window, all of
    /// them held, as [`gather`] reads them from the ring.
    fn with_outcomes(mut self, since: i64, outcomes: Vec<RunOutcome>) -> Self {
        let runs = self.runs.as_mut().unwrap();
        runs.window.events = outcomes
            .iter()
            .filter(|o| o.at_ms >= since)
            .cloned()
            .collect();
        runs.held = outcomes;
        self
    }
}

fn outcome(run: &str, at_ms: i64, provider: Option<&str>, kind: OutcomeKind) -> RunOutcome {
    RunOutcome {
        at_ms,
        run_id: run.into(),
        provider: provider.map(str::to_owned),
        credential: Some("credential".into()),
        kind,
    }
}
fn error(run: &str, at_ms: i64, class: Option<&str>, code: Option<&str>) -> RunOutcome {
    outcome(
        run,
        at_ms,
        Some("openai"),
        OutcomeKind::Error {
            class: class.map(str::to_owned),
            code: code.map(str::to_owned),
        },
    )
}
fn interrupted(run: &str, at_ms: i64, reason: Option<&str>) -> RunOutcome {
    outcome(
        run,
        at_ms,
        Some("openai"),
        OutcomeKind::Interrupted {
            reason: reason.map(str::to_owned),
        },
    )
}
fn paused(run: &str, at_ms: i64, reason: PauseReason) -> RunOutcome {
    outcome(run, at_ms, Some("openai"), OutcomeKind::Paused { reason })
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
    let mut evidence = healthy();
    evidence.unmapped_credentials = unmapped_credentials();
    let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
    let keys: Vec<_> = reply.findings.iter().map(|f| f.key.as_str()).collect();
    // An Antigravity account is routed like every subscription, so it lacking provider ids is
    // a finding; API keys, which may be provider-unmapped, and Cursor's quota-only row are not.
    assert_eq!(
        keys,
        [
            "antigravity-work",
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

/// A run still paused for sign-in is a finding, raised to an error after a day; one that
/// resumed since is not.
#[test]
fn diagnostics_auth_pauses_escalate_after_a_day_and_clear_on_resume() {
    for (age, severity) in [(100, Severity::Warn), (90_000_000, Severity::Error)] {
        let evidence = healthy().with_outcomes(
            0,
            vec![
                paused("auth-pause", NOW - age, PauseReason::AuthRequired),
                paused("resumed-pause", NOW - 300, PauseReason::AccountLimit),
                outcome(
                    "resumed-pause",
                    NOW - 200,
                    Some("openai"),
                    OutcomeKind::Resumed,
                ),
            ],
        );
        let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
        assert_eq!(reply.findings.len(), 1, "{:#?}", reply.findings);
        let pause = &reply.findings[0];
        assert_eq!(pause.check, "paused_runs");
        assert_eq!(pause.count, 1);
        assert_eq!(pause.severity, severity);
        assert_eq!(pause.samples[0]["run_id"], "auth-pause");
    }
}

#[test]
fn diagnostics_admission_never_reads_refused_routes() {
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
        let reply = answer(json!({}), Some(&caller), &stamp, NOW, |_since| {
            reads.set(reads.get() + 1);
            Ok(healthy())
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
    let since = NOW - DEFAULT_WINDOW_MS;
    let mut evidence = healthy().with_outcomes(
        since,
        vec![
            paused("paused", NOW - 90_000_000, PauseReason::AuthRequired),
            error(
                "billing",
                NOW - 100,
                Some("permanent"),
                Some("billing_exhausted"),
            ),
            error(
                "scattered",
                NOW - 90,
                Some("permanent"),
                Some("cyber_policy"),
            ),
            interrupted("interrupted", NOW - 70, None),
            interrupted("shutdown", NOW - 60, Some("runner_shutdown")),
            interrupted("crash", NOW - 50, Some("runner_crash")),
            error("outside", since - 1, Some("auth_required"), None),
        ],
    );
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
    let count = |check: &str| {
        reply
            .findings
            .iter()
            .filter(|f| f.check == check)
            .map(|f| f.count)
            .sum::<u64>()
    };
    // The billing error needs one occurrence; one scattered policy refusal is under its floor,
    // and the error before the window isn't counted.
    assert_eq!(count("run_errors"), 1);
    // The interruptions a restart writes (`runner_shutdown`, `runner_crash`) are not findings;
    // an interruption without a recorded reason is.
    assert_eq!(count("interrupted_runs"), 1);
    assert_eq!(
        reply
            .findings
            .iter()
            .find(|f| f.check == "interrupted_runs")
            .unwrap()
            .key,
        "unattributed"
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
    for (age, stale) in [(interval + 5_000, false), (2 * interval + 1, true)] {
        let mut evidence = healthy();
        evidence.catalog_age_ms = Ok(age);
        let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
        let found = reply.findings.iter().any(|f| f.check == "catalog_stale");
        assert_eq!(found, stale, "catalog aged {age} ms");
    }
}

#[test]
fn diagnostics_unreadable_sources_are_not_silent() {
    let mut evidence = healthy();
    evidence.runs = Err("unreadable".into());
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
        "paused_runs",
        "run_errors",
        "interrupted_runs",
    ] {
        assert!(
            reply.findings.iter().any(|f| f.check == check
                && f.key == "evidence_unavailable"
                && f.severity == Severity::Error),
            "{check}"
        );
    }
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
fn tool_result_json_dump_finding_shape() {
    let root = broca_test_support::scratch_root()
        .join(format!("diagnostics-json-dump-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let tally = broca_subc::tool_result_diagnostics::Tally::new(NOW - 1000);
    for offset in 1..=7 {
        // The oldest result comes from a second session, so the summary counts sessions.
        let session = if offset == 7 {
            "second-session"
        } else {
            "sample-session"
        };
        tally.record(
            NOW - offset,
            "tools",
            "search",
            session,
            Some("call-key"),
            200,
        );
    }
    tally.record(NOW - 8, "other-tools", "search", "other-session", None, 300);
    let head = scope(principal(), ScopeKind::Head, false, true);
    let reply = answer(json!({}), Some(&principal()), &head, NOW, |since| {
        let mut evidence = healthy();
        evidence.tool_result_json_dumps = tally.snapshot(since, NOW);
        Ok(evidence)
    })
    .unwrap();
    assert_eq!(reply.findings.len(), 2);
    let f = &reply.findings[0];
    assert_eq!(f.check, "tool_result_json_dump");
    assert_eq!(f.key, r#"["tools","search"]"#);
    assert_eq!(f.severity, Severity::Warn);
    assert_eq!(
        (f.count, f.first_seen_ms, f.last_seen_ms),
        (7, NOW - 7, NOW - 1)
    );
    // Digests print the summary and hint, never the key, so both must name the tool and module.
    assert_eq!(
        f.summary,
        "search from tools: 7 results in 2 sessions reached the model as a raw JSON dump instead of readable text."
    );
    assert_eq!(
        f.hint,
        "Report it to the owner of module tools with a sample call_key. The provider should shape its output into concise text for the model."
    );
    assert_eq!(
        reply.findings[1].summary,
        "search from other-tools: 1 result in 1 session reached the model as a raw JSON dump instead of readable text."
    );
    assert_eq!(
        f.samples,
        vec![
            json!({
                "module_id":"tools", "tool_name":"search", "session":"sample-session",
                "call_key":"call-key", "text_bytes":200, "coverage_since_ms":NOW - 1000
            });
            5
        ]
    );
    assert_eq!(reply.findings[1].count, 1);
    assert_eq!(reply.findings[1].key, r#"["other-tools","search"]"#);
    assert!(reply.findings[1].samples[0]["call_key"].is_null());
    vector("reply-tool-result-json-dump.json", json!({"result":reply}));
}

#[test]
fn tool_result_json_dump_unavailable_evidence_is_not_clean() {
    let root = broca_test_support::scratch_root().join(format!(
        "diagnostics-json-unavailable-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut evidence = healthy();
    evidence.tool_result_json_dumps = Err("poisoned".into());
    let reply = report(evidence, NOW, 1000);
    assert_eq!(reply.findings.len(), 1);
    assert_eq!(reply.findings[0].check, "tool_result_json_dump");
    assert_eq!(reply.findings[0].key, "evidence_unavailable");
    assert_eq!(reply.findings[0].severity, Severity::Error);
}

#[test]
fn diagnostics_findings_v1_vectors_match_serving_replies() {
    let root = broca_test_support::scratch_root()
        .join(format!("diagnostics-vectors-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
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
    let reply = answer(json!({}), Some(&principal()), &head, NOW, |_since| {
        Ok(healthy())
    })
    .unwrap();
    assert!(reply.findings.is_empty());
    assert_eq!(reply.coverage["send_refusals"].since_ms, NOW - 1000);
    vector("reply-healthy.json", json!({"result":reply}));
    let outcomes = vec![
        paused("paused", NOW - 90_000_000, PauseReason::AuthRequired),
        error(
            "billing",
            NOW - 100,
            Some("permanent"),
            Some("billing_exhausted"),
        ),
        outcome(
            "unattributed-error",
            NOW - 200,
            None,
            OutcomeKind::Error {
                class: Some("auth_required".into()),
                code: None,
            },
        ),
    ];
    let reply = answer(json!({}), Some(&principal()), &head, NOW, |since| {
        let mut evidence = healthy().with_outcomes(since, outcomes);
        evidence.unmapped_credentials = unmapped_credentials();
        evidence.rings.insert(
            "session_address_unavailable".into(),
            Ok(Snapshot {
                since_ms: NOW - 1000,
                truncated: false,
                events: ["incomplete_inventory", "gap_marker", "store_error"]
                    .into_iter()
                    .map(|reason| Event {
                        at_ms: NOW - 1,
                        module: String::new(),
                        code: reason.into(),
                    })
                    .collect(),
            }),
        );
        Ok(evidence)
    })
    .unwrap();
    vector("reply-findings.json", json!({"result":reply}));
}

#[test]
fn diagnostics_error_floors_require_bursts_but_auth_requires_one() {
    let mut outcomes = Vec::new();
    for (class, code, total) in [
        ("permanent", "request_invalid", 9),
        ("transient", "rate_limited", 49),
        ("context_overflow", "context_length_exceeded", 24),
        ("auth_required", "invalid_api_key", 1),
    ] {
        for n in 0..total {
            outcomes.push(error(
                &format!("{class}-{n}"),
                NOW - 100,
                Some(class),
                Some(code),
            ));
        }
    }
    let since = NOW - DEFAULT_WINDOW_MS;
    let errors = |outcomes: &[RunOutcome]| {
        report(
            healthy().with_outcomes(since, outcomes.to_vec()),
            NOW,
            DEFAULT_WINDOW_MS,
        )
    };
    let before = errors(&outcomes);
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
        outcomes.push(error(
            &format!("{class}-{n}"),
            NOW - 100,
            Some(class),
            Some(code),
        ));
    }
    let after = errors(&outcomes);
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
    std::fs::write(root.join("late-results-rebuild-required"), b"").unwrap();
    let evidence = gather(&root, Ok(0), 0, live::now_ms()).unwrap();
    let reply = report(evidence, live::now_ms(), DEFAULT_WINDOW_MS);
    assert!(reply
        .findings
        .iter()
        .any(|f| f.check == "late_results_rebuild_required" && f.severity == Severity::Error));
}

#[test]
fn diagnostics_names_repeated_held_delivery_failure() {
    let mut evidence = healthy();
    evidence.late_delivery_stuck =
        vec![json!({"module":"basal", "call_key":"lineage:4", "attempts":3})];
    let reply = report(evidence, NOW, DEFAULT_WINDOW_MS);
    let finding = reply
        .findings
        .iter()
        .find(|f| f.check == "late_results_delivery_stuck")
        .expect("repeated delivery failure needs a named finding");
    assert_eq!(finding.severity, Severity::Error);
    assert_eq!(finding.count, 1);
    assert_eq!(finding.samples[0]["attempts"], 3);
}
