use super::*;
use crate::system_one_fact::UsageExportSegment;
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{AttemptOutcome, ContentProgress},
    request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
    MessageId, RunId, SessionScope, StepId,
};
use broca_wal::{RunConfig, RunOrigin};
use serde_json::json;

const LINEAGE: &str = "0f0e0d0c-0b0a-4908-8706-050403020100";

fn frame(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

fn session() -> BindIdentity {
    BindIdentity::new("/tmp/project", "runner", "session")
}

fn identity() -> StepFactIdentity {
    StepFactIdentity::new(LINEAGE, &session())
}

fn started(
    seq: u64,
    run_id: &str,
    ts_ms: Option<u64>,
    scope: Option<SessionScope>,
) -> FramedRecord {
    frame(
        seq,
        WalRecord::RunStarted {
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId(run_id.into()),
            session: session(),
            ts_ms,
            config: Box::new(RunConfig {
                turn_directives_v1: None,
                render_tags: false,
                computed_tags: None,
                model: ModelHandle::new("anthropic", "claude-sonnet-4-5"),
                tools: vec![].into(),
                tool_choice: ToolChoice::Auto,
                generation: GenerationConfig::default(),
                response_format: ResponseFormat::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: json!({
                    "family_id": "anthropic_messages",
                    "cache": {"version": 1, "class": "markers", "syntax": "anthropic_cache_control",
                              "ttl": "1h", "max_breakpoints": 4, "lookback_blocks": 20}
                }),
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: Some(200_000),
                work_class: None,
                variant: Some("high".into()),
                service_tier: None,
                stop_when: vec![],
                context_management: None,
            }),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "secret prompt".into(),
                }],
            )]
            .into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        },
    )
}

fn attempt(
    seq: u64,
    step: u64,
    ordinal: u32,
    sent: u64,
    first_content: Option<u64>,
    end: u64,
) -> FramedRecord {
    frame(
        seq,
        WalRecord::ModelAttemptFinished {
            step_id: StepId(step),
            attempt: ordinal,
            request_body_len: 1000 + u64::from(ordinal),
            request_body_sha256: format!("{ordinal:064x}"),
            http_status: Some(200),
            content_type: None,
            response_body_len: 10,
            response_body_sha256: "b".repeat(64),
            response_body_cap: None,
            outcome: AttemptOutcome::Complete,
            ts_ms: end,
            content_progress: Box::new(ContentProgress {
                first_content_at_ms: first_content,
                ..ContentProgress::default()
            }),
            sent_at_ms: Some(sent),
            transport: None,
            websocket_request: None,
            websocket_close_code: None,
            input_transformations: None,
            input_transformations_unmeasured: false,
            credential_refresh: None,
            served_account_ref: None,
        },
    )
}

fn step(seq: u64, id: u64, usage: Usage, retries_used: u32) -> FramedRecord {
    frame(
        seq,
        WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: StepId(id),
            assistant_message: AssistantMessage {
                message_id: MessageId(format!("m{id}")),
                content: vec![ContentBlock::Text {
                    text: "secret answer".into(),
                }],
            }
            .into(),
            usage,
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used,
            service_tier_used: None,
            speed_used: None,
        },
    )
}

fn fold_all(records: &[FramedRecord]) -> Vec<FoldedStep> {
    let mut fold = ModelStepFold::default();
    let identity = identity();
    records
        .iter()
        .filter_map(|frame| fold.apply(frame, &identity))
        .collect()
}

#[test]
fn fact_id_is_the_documented_length_prefixed_digest() {
    // Computed independently from the documented encoding (Python hashlib over the bytes).
    assert_eq!(
        model_step_fact_id(LINEAGE, "run-1", 2),
        "98ad7d92fbe62636e9403aad250cb22c44495094efc8d3fac585c3c0a94740cd"
    );
    assert_ne!(
        model_step_fact_id(LINEAGE, "run-1", 2),
        model_step_fact_id(LINEAGE, "run-1", 3)
    );
}

#[test]
fn a_committed_step_carries_its_committed_attempt_and_the_runs_frozen_context() {
    let scope: SessionScope = serde_json::from_value(json!({
        "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
        "ref": "r", "scope_epoch": 1, "agent_id": "agent-7", "run_id": "agent-run-9"
    }))
    .unwrap();
    let usage = Usage {
        input_tokens: Some(12),
        cached_input_tokens: Some(30_000),
        cache_write_tokens: Some(0),
        output_tokens: Some(40),
        reasoning_tokens: None,
    };
    let folded = fold_all(&[
        started(1, "run-1", Some(1_000), Some(scope)),
        frame(2, WalRecord::StepStarted { step_id: StepId(1) }),
        attempt(3, 1, 0, 2_000, None, 2_500),
        attempt(4, 1, 1, 3_000, Some(3_400), 4_000),
        step(5, 1, usage, 1),
    ]);
    assert_eq!(folded.len(), 1);
    assert_eq!(folded[0].age_basis_ms, Some(4_000));
    let json = serde_json::to_string(&folded[0].fact).unwrap();
    // The committed (second) attempt's body digest, as the fixture spells it.
    let committed_digest = format!("{:064x}", 1);
    let expected = format!(
        concat!(
            r#"{{"kind":"model_step","fact_id":"{}","lineage_id":"{}","run_id":"run-1","step_id":1,"#,
            r#""session_ref":"{}","session":{{"project_root":"/tmp/project","harness":"runner","session":"session"}},"#,
            r#""agent_id":"agent-7","agent_run_id":"agent-run-9","provider":"anthropic","model":"claude-sonnet-4-5","#,
            r#""variant":"high","wire_family":"anthropic_messages","#,
            r#""usage":{{"input_tokens":12,"cached_input_tokens":30000,"cache_write_tokens":0,"output_tokens":40}},"#,
            r#""finish_reason":"tool_calls","attempts":2,"retried":true,"request_body_len":1001,"#,
            r#""request_body_sha256":"{}","started_at_ms":2000,"first_content_at_ms":3400,"#,
            r#""finished_at_ms":4000,"context_limit":200000,"#,
            r#""cache_policy":{{"class":"markers","ttl":"1h","max_breakpoints":4}}}}"#
        ),
        model_step_fact_id(LINEAGE, "run-1", 1),
        LINEAGE,
        crate::wal_writer::agent_session_ref(&session()),
        committed_digest,
    );
    assert_eq!(json, expected);
    assert!(!json.contains("secret"), "a step fact carries no content");
}

#[test]
fn absent_measurements_stay_absent_and_a_step_without_attempt_records_dates_from_its_run() {
    let folded = fold_all(&[
        started(1, "run-1", Some(1_000), None),
        step(2, 1, Usage::default(), 0),
    ]);
    let fact = &folded[0].fact;
    let value = serde_json::to_value(fact).unwrap();
    assert_eq!(
        value["usage"],
        json!({}),
        "unreported members are absent, never zero"
    );
    for absent in [
        "request_body_len",
        "request_body_sha256",
        "started_at_ms",
        "first_content_at_ms",
        "finished_at_ms",
        "agent_id",
        "agent_run_id",
    ] {
        assert!(value.get(absent).is_none(), "{absent} must be absent");
    }
    assert_eq!((fact.attempts, fact.retried), (1, false));
    assert_eq!(folded[0].age_basis_ms, Some(1_000));
}

#[test]
fn attempts_cut_short_by_an_auth_pause_count_toward_the_resumed_step() {
    let folded = fold_all(&[
        started(1, "run-1", Some(1_000), None),
        attempt(2, 1, 0, 2_000, None, 2_100),
        frame(
            3,
            WalRecord::RunPaused {
                run_id: RunId("run-1".into()),
                ts_ms: Some(2_200),
                reason: broca_protocol::PauseReason::AuthRequired,
                error: None,
            },
        ),
        attempt(4, 1, 0, 9_000, Some(9_100), 9_500),
        step(5, 1, Usage::default(), 0),
    ]);
    assert_eq!(folded.len(), 1);
    let fact = &folded[0].fact;
    assert_eq!((fact.step_id, fact.attempts, fact.retried), (1, 2, true));
    assert_eq!(fact.started_at_ms, Some(2_000));
    assert_eq!(fact.finished_at_ms, Some(9_500));
    assert_eq!(fact.request_body_len, Some(1_000));
}

#[test]
fn steps_outside_a_known_run_yield_nothing() {
    let folded = fold_all(&[
        step(1, 4, Usage::default(), 0),
        started(2, "run-1", None, None),
        step(3, 1, Usage::default(), 0),
        frame(
            4,
            WalRecord::RunFinished {
                interrupt_reason: None,
                reason: broca_protocol::caller::RunFinishReason::Completed,
                ts_ms: None,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        ),
        step(5, 2, Usage::default(), 0),
    ]);
    assert_eq!(folded.len(), 1);
    assert_eq!(folded[0].fact.step_id, 1);
    assert_eq!(folded[0].age_basis_ms, None);
}

#[test]
fn a_persisted_fold_continues_exactly_where_it_stopped() {
    let records = [
        started(1, "run-1", Some(1_000), None),
        attempt(2, 1, 0, 2_000, None, 2_100),
        step(3, 1, Usage::default(), 0),
        attempt(4, 2, 0, 3_000, None, 3_100),
        attempt(5, 2, 1, 3_200, Some(3_300), 3_400),
        step(6, 2, Usage::default(), 1),
    ];
    let whole = fold_all(&records);
    let identity = identity();
    let mut fold = ModelStepFold::default();
    let mut split = Vec::new();
    for (index, frame) in records.iter().enumerate() {
        if index == 4 {
            let json = serde_json::to_string(&fold).unwrap();
            fold = serde_json::from_str(&json).unwrap();
        }
        split.extend(fold.apply(frame, &identity));
    }
    assert_eq!(split, whole);
}

#[test]
fn the_export_decoder_tells_step_facts_from_session_facts() {
    let fact = fold_all(&[
        started(1, "run-1", None, None),
        step(2, 1, Usage::default(), 0),
    ])
    .remove(0)
    .fact;
    let json = serde_json::to_string(&fact).unwrap();
    let decoded: UsageExportSegment = serde_json::from_str(&json).unwrap();
    assert!(decoded.as_session().is_none());
    let UsageExportSegment::ModelStep(decoded_fact) = &decoded else {
        panic!("not a model step fact")
    };
    assert_eq!(**decoded_fact, fact);
    assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
}
