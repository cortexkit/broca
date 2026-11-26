use broca_core::billing::{
    attempt::{compute_attempt_facts, AttemptEvidence, RunAttemptFacts},
    BillingEvent, BillingRecord,
};
use broca_core::{ChargeBasis, SpendSegment};
use broca_protocol::{
    caller::RunFinishReason,
    request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
    BindIdentity, BindPrincipal, RunId, SessionScope, StepId, Usage,
};
use broca_records::record::{AuthSelection, RunConfig};
use broca_store::system_one::{
    BatchClaim, BatchScope, Claimed, FactContext, Outcome, PaidRecord, StoredOutcome,
};
use broca_store::SystemOneStore;

fn run_config() -> RunConfig {
    RunConfig {
        global_system: None,
        account_route: None,
        route_decision_id: None,
        served_provider: None,
        account_limit_pause: false,
        turn_directives_v1: None,
        model: ModelHandle::new("anthropic", "claude"),
        tools: vec![].into(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        response_format: ResponseFormat::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: Some(AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "apikey:anthropic".into(),
            account_ref: Some("acct1".into()),
            resolved_credential_id: Some("apikey:anthropic".into()),
            vault_request: None,
            account_decision: None,
        }),
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        context_management: None,
        render_tags: false,
        computed_tags: None,
    }
}

fn session_facts(flow: bool) -> RunAttemptFacts {
    let run_id = if flow { "flow-run" } else { "agent-run" };
    let scope = SessionScope {
        owner: BindPrincipal::Reserved {
            module_id: "synthetic-owner".into(),
        },
        scope_ref: "synthetic-scope".into(),
        scope_epoch: 1,
        flow_id: flow.then(|| "flow-1".into()),
        sender: None,
        agent_id: (!flow).then(|| "agent-1".into()),
        run_id: (!flow).then(|| "agent-run-1".into()),
        agent_id_frozen: false,
    };
    let records = vec![
        BillingRecord {
            seq: 10,
            event: BillingEvent::RunStarted {
                run_id: RunId(run_id.into()),
                session: BindIdentity::new(
                    "/synthetic/fact-owners",
                    "synthetic-harness",
                    "synthetic-session",
                ),
                config: Box::new(run_config()),
                ts_ms: Some(1_000),
                scope: Some(scope),
            },
        },
        BillingRecord {
            seq: 20,
            event: BillingEvent::StepFinished {
                step_id: StepId(1),
                written_at_ms: None,
                usage: Usage {
                    input_tokens: Some(11),
                    output_tokens: Some(7),
                    ..Usage::default()
                },
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
                fallback: Default::default(),
            },
        },
        BillingRecord {
            seq: 30,
            event: BillingEvent::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms: Some(1_030),
                retries_used: 0,
            },
        },
    ];
    let attempts = [AttemptEvidence {
        seq: 15,
        step_id: 1,
        attempt: 1,
        attempt_id: Some("attempt-1".into()),
        outcome: "complete".into(),
        http_status: Some(200),
        response_bytes: 128,
        content_received: true,
        sent_at_ms: Some(1_010),
        recorded_at_ms: 1_015,
        served_account_ref: None,
        input_transformations: None,
    }];
    compute_attempt_facts(
        &records,
        &RunId(run_id.into()),
        &attempts,
        "synthetic-source",
        &|_: &str, _: &str| (ChargeBasis::MeteredApi, 1),
    )
}

fn system_one_fact(
    scope: BatchScope,
    batch_id: &str,
) -> broca_core::system_one_fact::SystemOneFact {
    let store = SystemOneStore::open_in_memory().unwrap();
    assert_eq!(
        store
            .claim(&BatchClaim {
                role: "classifier",
                batch_id,
                model: "typesafe/jev-1.13.0",
                identity: "{}",
                body_digest: "synthetic-digest",
                frozen_row: "{}",
                scope: &scope,
                ceiling_nanos: None,
                owner: "synthetic-process",
                now_ms: 2_000,
            })
            .unwrap(),
        Claimed::Fresh
    );
    let context = FactContext {
        source_instance_id: "synthetic-source".into(),
        provider: "typesafe".into(),
        model: "jev-1.13.0".into(),
        auth_selection: None,
        account_ref: None,
        charge_basis: ChargeBasis::MeteredApi,
        charge_basis_mapping_version: 1,
        charge_basis_overrides: "none".into(),
        spend_nanos: 4_200,
    };
    assert!(store
        .record_paid(
            "classifier",
            batch_id,
            "synthetic-process",
            &StoredOutcome {
                index: 0,
                outcome: Outcome::Answered("{}".into()),
                input_tokens: Some(11),
                output_tokens: Some(7),
            },
            &PaidRecord {
                spend_nanos: 4_200,
                now_ms: 2_030,
                fact: Some(&context),
            },
        )
        .unwrap());
    store.pending_facts().unwrap().remove(0).fact
}

#[test]
fn flow_stamped_segment_fact_carries_flow_id() {
    let facts = session_facts(true);
    assert_eq!(facts.segments.len(), 1);
    let segment = serde_json::to_value(&facts.segments[0]).unwrap();
    assert_eq!(segment["flow_id"], "flow-1");
    assert!(segment.get("agent_id").is_none(), "{segment}");
    assert!(segment.get("agent_run_id").is_none(), "{segment}");
}

#[test]
fn flow_stamped_attempt_fact_carries_flow_id() {
    let facts = session_facts(true);
    assert_eq!(facts.attempts.len(), 1);
    let attempt = serde_json::to_value(&facts.attempts[0]).unwrap();
    assert_eq!(attempt["flow_id"], "flow-1");
    assert!(attempt.get("agent_id").is_none(), "{attempt}");
    assert!(attempt.get("agent_run_id").is_none(), "{attempt}");
}

#[test]
fn agent_stamped_session_facts_keep_their_agent_ids_without_a_flow_id() {
    let facts = session_facts(false);
    for fact in [
        serde_json::to_value(&facts.segments[0]).unwrap(),
        serde_json::to_value(&facts.attempts[0]).unwrap(),
    ] {
        assert_eq!(fact["agent_id"], "agent-1");
        assert_eq!(fact["agent_run_id"], "agent-run-1");
        assert_eq!(fact["auth_selection"], "apikey:anthropic");
        assert_eq!(fact["resolved_credential_id"], "apikey:anthropic");
        assert_eq!(fact["account_ref"], "acct1");
        assert!(fact.get("flow_id").is_none(), "{fact}");
    }
}

#[test]
fn system_one_facts_name_the_batch_scopes_agent_or_flow() {
    let agent = serde_json::to_value(system_one_fact(
        BatchScope::Run {
            agent_id: "agent-1".into(),
            run_id: "agent-run-1".into(),
        },
        "agent-batch",
    ))
    .unwrap();
    assert_eq!(agent["agent_id"], "agent-1");
    assert_eq!(agent["run_id"], "agent-run-1");
    assert!(agent.get("flow_id").is_none(), "{agent}");

    let flow = serde_json::to_value(system_one_fact(
        BatchScope::Flow {
            flow_id: "flow-1".into(),
        },
        "flow-batch",
    ))
    .unwrap();
    assert_eq!(flow["flow_id"], "flow-1");
    assert!(flow.get("agent_id").is_none(), "{flow}");
    assert!(flow.get("run_id").is_none(), "{flow}");
}

#[test]
fn facts_without_new_owner_values_keep_their_pinned_bytes() {
    // These are the shapes stored before caller-owner members were added. Deserializing and
    // serializing them proves the new optional members stay absent instead of changing old facts.
    const SEGMENT: &str = r#"{"run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"segment_ordinal":0,"fact_id":"e1f385e6803bfab6a51fbd03917e2927e6f68a99ea34642b3dbc1fff7ab5bf0e","provider":"anthropic","model":"claude","auth_selection":"chatgpt:openai","account_ref":"acct-9","charge_basis":"unknown","charge_basis_mapping_version":1,"usage":{"input_tokens":10,"output_tokens":2},"retries_used":0,"pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#;
    const ATTEMPT: &str = r#"{"kind":"model_attempt","fact_id":"attempt-fact","run_id":"run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"step_id":1,"attempt":1,"outcome":"complete","provider":"anthropic","model":"claude","reasoning_included_in_output":true,"requested_service_tier":"standard","content_received":true,"response_bytes":512,"charge_basis":"metered_api","charge_basis_mapping_version":1,"charge_basis_overrides":"none","recorded_at_ms":2000}"#;
    const SYSTEM_ONE: &str = r#"{"kind":"system_one","fact_id":"fc713f0ed72c3b03d4e734619bc163705fe49eb4797815f0aa84e7a5e588c8c9","batch_id":"paid-flow","item_index":0,"provider":"typesafe","model":"jev-1.13.0","auth_selection":"apikey:typesafe:primary","account_ref":"primary","flow_id":"flow-1","charge_basis":"metered_api","charge_basis_mapping_version":6,"charge_basis_overrides":"none","usage":{"input_tokens":100},"occurred_at_ms":1792772000000}"#;

    let segment: SpendSegment = serde_json::from_str(SEGMENT).unwrap();
    assert_eq!(serde_json::to_string(&segment).unwrap(), SEGMENT);
    let attempt: broca_core::billing::attempt::AttemptFact = serde_json::from_str(ATTEMPT).unwrap();
    assert_eq!(serde_json::to_string(&attempt).unwrap(), ATTEMPT);
    let system_one: broca_core::system_one_fact::SystemOneFact =
        serde_json::from_str(SYSTEM_ONE).unwrap();
    assert_eq!(serde_json::to_string(&system_one).unwrap(), SYSTEM_ONE);
}

/// With `BROCA_FACT_OWNER_SAMPLES` naming a directory, write one fact of each kind for an
/// agent-stamped caller and a flow-stamped caller. Normal test runs only exercise the assertions.
#[test]
fn write_fact_owner_samples_when_requested() {
    let Some(dir) = std::env::var_os("BROCA_FACT_OWNER_SAMPLES") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (stamp, flow, scope) in [
        (
            "agent",
            false,
            BatchScope::Run {
                agent_id: "agent-1".into(),
                run_id: "agent-run-1".into(),
            },
        ),
        (
            "flow",
            true,
            BatchScope::Flow {
                flow_id: "flow-1".into(),
            },
        ),
    ] {
        let facts = session_facts(flow);
        let system_one = system_one_fact(scope, &format!("{stamp}-batch"));
        for (kind, json) in [
            (
                "segment",
                serde_json::to_string(&facts.segments[0]).unwrap(),
            ),
            (
                "attempt",
                serde_json::to_string(&facts.attempts[0]).unwrap(),
            ),
            ("system-one", serde_json::to_string(&system_one).unwrap()),
        ] {
            std::fs::write(dir.join(format!("{stamp}-{kind}.json")), json + "\n").unwrap();
        }
    }
}
