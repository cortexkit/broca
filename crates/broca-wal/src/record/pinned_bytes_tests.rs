//! The exact bytes every record kind is written with today.
//!
//! Every session already on disk holds these shapes, and a build rolled back to after a newer
//! one ran must still read what the newer one wrote. So an optional field added to any record
//! must cost these bytes nothing, and a new record kind must leave every existing kind's bytes
//! alone. Each pinned record is built only by decoding its bytes, never by a struct literal, so
//! this file does not change when a field is added: a new field that encoded an absent value
//! (a `null`, an empty list, a default) fails here.
//!
//! [`tag_of`] matches every variant with no wildcard, so adding a record kind fails to compile
//! here until the new kind is given its own pinned bytes below.

use super::*;

/// Cancellation must not change the bytes or reader requirements of existing boundaries.
#[test]
fn pre_cancel_at_boundaries_keep_their_bytes_and_frame_requirements() {
    use crate::framing::{encode, FRAMING_VERSION, HEADER_LEN};

    for pinned in [
        r#"{"type":"step_started","step_id":1}"#,
        r#"{"type":"tool_batch_finished","batch_id":"r-b1"}"#,
        r#"{"type":"turn_finished","step_id":1}"#,
        r#"{"type":"run_paused","run_id":"r","reason":"auth_required"}"#,
        r#"{"type":"run_finished","reason":"cancelled","usage":{},"retries_used":0}"#,
    ] {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        assert!(record.required_features().is_empty());
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], FRAMING_VERSION);
        assert_eq!(&frame[HEADER_LEN..], pinned.as_bytes());
    }
}

#[test]
fn attempt_bytes_before_content_progress_stay_identical() {
    let pinned = include_str!("../../tests/fixtures/model_attempt_before_progress.json").trim_end();
    let record: WalRecord = serde_json::from_str(pinned).unwrap();
    assert!(matches!(record, WalRecord::ModelAttemptFinished { .. }));
    assert!(record.required_features().is_empty());
    assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
}

#[test]
fn attempt_progress_fields_roundtrip_without_a_reader_feature() {
    let pinned = include_str!("../../tests/fixtures/model_attempt_with_progress.json").trim_end();
    let record: WalRecord = serde_json::from_str(pinned).unwrap();
    let WalRecord::ModelAttemptFinished {
        content_progress, ..
    } = &record
    else {
        panic!("attempt")
    };
    assert_eq!(content_progress.first_content_at_ms, Some(1790000001000));
    assert_eq!(content_progress.events_after_last_content, Some(1));
    assert!(record.required_features().is_empty());
    assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
}

/// The wire tag of each record kind. Exhaustive on purpose: see the module comment.
fn tag_of(record: &WalRecord) -> &'static str {
    match record {
        WalRecord::RunStarted { .. } => "run_started",
        WalRecord::ModelStepFinished { .. } => "model_step_finished",
        WalRecord::ModelAttemptFinished { .. } => "model_attempt_finished",
        WalRecord::StepStarted { .. } => "step_started",
        WalRecord::ToolBatchStarted { .. } => "tool_batch_started",
        WalRecord::ToolDispatchIntent { .. } => "tool_dispatch_intent",
        WalRecord::ToolNoDispatch { .. } => "tool_no_dispatch",
        WalRecord::ToolResult { .. } => "tool_result",
        WalRecord::ToolBatchFinished { .. } => "tool_batch_finished",
        WalRecord::TurnFinished { .. } => "turn_finished",
        WalRecord::BudgetFired { .. } => "budget_fired",
        WalRecord::RunFinished { .. } => "run_finished",
        WalRecord::RunCancelRequested { .. } => "run_cancel_requested",
        WalRecord::RunPaused { .. } => "run_paused",
        WalRecord::RunConfigRefrozen { .. } => "run_config_refrozen",
        WalRecord::RunAuthRefrozen { .. } => "run_auth_refrozen",
        WalRecord::Queued { .. } => "queued",
        WalRecord::Steered { .. } => "steered",
        WalRecord::Retracted { .. } => "retracted",
        WalRecord::ApprovalRequested { .. } => "approval_requested",
        WalRecord::ApprovalResolved { .. } => "approval_resolved",
        WalRecord::ResumeStarted { .. } => "resume_started",
        WalRecord::CacheWarmed { .. } => "cache_warmed",
        WalRecord::CompactionRequest { .. } => "compaction_request",
        WalRecord::CompactionMessage { .. } => "compaction_message",
        WalRecord::CompactionAnswer { .. } => "compaction_answer",
        WalRecord::CompactionWait { .. } => "compaction_wait",
        WalRecord::ToolCallDeferred { .. } => "tool_call_deferred",
        WalRecord::ApprovalRecorded { .. } => "approval_recorded",
        WalRecord::AwaitingApproval { .. } => "awaiting_approval",
        WalRecord::LateApprovalRecorded { .. } => "late_approval_recorded",
        WalRecord::LateDispatchIntent { .. } => "late_dispatch_intent",
        WalRecord::LateResult { .. } => "late_result",
        WalRecord::QuestionWithdrawalAcknowledged { .. } => "question_withdrawal_acknowledged",
        WalRecord::InputTokenCapFrozen { .. } => "input_token_cap_frozen",
        WalRecord::InputTokenBudgetExhausted { .. } => "input_token_budget_exhausted",
        WalRecord::Unknown => "unknown",
    }
}

/// Every record kind this build writes, each with its optional fields both present and absent
/// where it has any. `Unknown` is absent because it can never be written.
const PINNED: &[&str] = &[
    r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"direct"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-7"}"#,
    r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
    r#"{"type":"model_step_finished","step_id":3,"assistant_message":{"message_id":"msg-3","content":[{"type":"text","text":"ok"},{"type":"tool_call","tool_call_id":"t1","tool_name":"edit","input":{"f":"a"}}]},"usage":{"input_tokens":120,"output_tokens":9},"finish_reason":"tool_calls","retries_used":1,"service_tier_used":"priority","speed_used":"fast"}"#,
    r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[]},"usage":{},"retries_used":0}"#,
    r#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":503,"content_type":"text/plain","response_body_len":1048576,"response_body_sha256":"bb","response_body_cap":1048576,"outcome":"http_error","ts_ms":5}"#,
    r#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":0,"response_body_sha256":"e3b0","outcome":"connect_failed","ts_ms":5}"#,
    r#"{"type":"model_attempt_finished","step_id":4,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"content_type":"text/event-stream","response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":1790000000000,"input_transformations":[{"type":"thinking_dropped","path":"messages.1.content.0","reason":"prefix_binding_mismatch"}],"credential_refresh":{"vault_op":"get_scoped","min_ttl_ms":60000,"force_refresh":false,"record_version":3},"served_account_ref":"acct-1"}"#,
    r#"{"type":"model_attempt_finished","step_id":4,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":1790000000000,"sent_at_ms":1789999940000}"#,
    r#"{"type":"model_attempt_finished","step_id":55,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"content_type":"text/event-stream","response_body_len":40,"response_body_sha256":"bb","outcome":"complete","ts_ms":5,"credential_refresh":{"vault_op":"get_scoped","min_ttl_ms":600000,"force_refresh":false,"record_version":236},"served_account_ref":"acct-1"}"#,
    r#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":40,"response_body_sha256":"bb","outcome":"complete","ts_ms":5,"credential_refresh":{"vault_op":"get","force_refresh":true}}"#,
    r#"{"type":"model_attempt_finished","step_id":55,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":40,"response_body_sha256":"bb","outcome":"complete","ts_ms":5,"credential_refresh":{"vault_op":"get_scoped","min_ttl_ms":600000,"force_refresh":false,"record_version":237,"reason":"expiring"}}"#,
    r#"{"type":"step_started","step_id":2}"#,
    r#"{"type":"tool_batch_started","batch_id":"r-b1","step_id":1,"planned_calls":["t1","t2"]}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"}}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","args":null}"#,
    r#"{"type":"tool_no_dispatch","batch_id":"r-b1","tool_call_id":"t2","reason":{"kind":"denied","reason":"policy says no"}}"#,
    r#"{"type":"tool_no_dispatch","batch_id":"r-b1","tool_call_id":"t2","reason":{"kind":"validation_failed","detail":"bad input"}}"#,
    r#"{"type":"tool_no_dispatch","batch_id":"r-b1","tool_call_id":"t2","reason":{"kind":"unavailable","detail":"route gone"}}"#,
    r#"{"type":"tool_no_dispatch","batch_id":"r-b1","tool_call_id":"t2","reason":{"kind":"policy_blocked","detail":"blocked"}}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool not dispatched: route gone"},"is_error":true},"outcome":"completed"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"route_drained: scope_ended"},"is_error":true},"outcome":"completed","reason":"scope_ended"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool outcome unknown (interrupted for review): route closed"},"is_error":true},"outcome":"unknown","reason":"scope_parent_ended"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool outcome unknown (interrupted for review): route closed"},"is_error":true},"outcome":"unknown"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t2","output":{"kind":"error","message":"tool not run: batch interrupted"},"is_error":true},"outcome":"interrupted"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t2","output":{"kind":"error","message":"tool not run: max steps reached before dispatch"},"is_error":true},"outcome":"max_steps"}"#,
    r#"{"type":"tool_batch_finished","batch_id":"r-b1"}"#,
    r#"{"type":"turn_finished","step_id":1}"#,
    r#"{"type":"budget_fired","run_id":"r","kind":"nudge","threshold":2,"ordinal":4,"caller_text":"Check the work."}"#,
    r#"{"type":"budget_fired","run_id":"r","kind":"finalize","threshold":9,"ordinal":7}"#,
    r#"{"type":"run_finished","reason":"error","ts_ms":1790000000000,"usage":{},"error":{"class":"transient","message":"429: rate limited","status":429,"retry_after_secs":353271.0,"provider_code":"rate_limit_error"},"retries_used":1}"#,
    r#"{"type":"run_finished","reason":"interrupted","usage":{"input_tokens":10,"output_tokens":2},"retries_used":0}"#,
    r#"{"type":"run_finished","reason":"cancelled","ts_ms":1790000000000,"usage":{},"retries_used":0}"#,
    r#"{"type":"run_paused","run_id":"r1","ts_ms":1790000000001,"reason":"auth_required","error":{"class":"auth_required","message":"needs reauth","status":401,"provider_code":"authentication_error"}}"#,
    r#"{"type":"run_paused","run_id":"r1","reason":"auth_required"}"#,
    r#"{"type":"run_config_refrozen","renderer_version":2,"ts_ms":5,"frozen_render_config":{"project_id":"gcp-1"}}"#,
    r#"{"type":"run_config_refrozen","renderer_version":2,"frozen_render_config":null}"#,
    r#"{"type":"run_auth_refrozen","ts_ms":5,"auth_selection":{"credential_id":"oauth:anthropic:work","account_ref":"acct-1"}}"#,
    r#"{"type":"run_auth_refrozen","auth_selection":{"credential_id":"oauth:anthropic:work"}}"#,
    r#"{"type":"queued","submission_id":"sub-run-8","request":{"project_id":"pj-a","model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","principal":{"kind":"direct"}}"#,
    r#"{"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#,
    r#"{"type":"queued","submission_id":"sub-run-9","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"stop and do this"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"send_id":"interrupt-1","delivery":"interrupt"}"#,
    r#"{"type":"steered","send_id":"steer-1","request_identity_digest":"v1:abc","delivery":"steer","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
    r#"{"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
    r#"{"type":"retracted","submission_id":"sub-1","failure":{"run_id":"r2","error":{"class":"permanent","message":"model not found"}}}"#,
    r#"{"type":"retracted","submission_id":"sub-1"}"#,
    r#"{"type":"approval_requested","tool_call_id":"t1"}"#,
    r#"{"type":"approval_resolved","tool_call_id":"t1","decision":{"decision":"allow"}}"#,
    r#"{"type":"approval_resolved","tool_call_id":"t1","decision":{"decision":"allow_narrowed","input":{"f":"a"}}}"#,
    r#"{"type":"approval_resolved","tool_call_id":"t1","decision":{"decision":"deny","reason":"no"}}"#,
    r#"{"type":"resume_started","episode":"r","resumed_from_seq":7,"replayed":{"messages":3,"model_steps":1,"tool_results":1},"redone":[{"kind":"model_step","step_id":2},{"kind":"tool_dispatch","tool_call_id":"t2"}],"indeterminate":["t1"]}"#,
    r#"{"type":"resume_started","episode":"r","resumed_from_seq":1,"replayed":{"messages":1,"model_steps":0,"tool_results":0},"redone":[],"indeterminate":[]}"#,
    r#"{"type":"model_attempt_finished","step_id":4,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":5,"transport":"websocket","websocket_request":{"len":37,"sha256":"cc"},"websocket_close_code":1012}"#,
    r#"{"type":"model_attempt_finished","step_id":4,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":5,"transport":"https"}"#,
    r#"{"type":"cache_warmed","run_id":"r","step_id":2,"request_body_sha256":"aa","warm_body_sha256":"bb","usage":{"input_tokens":3,"cached_input_tokens":4000,"cache_write_tokens":0},"served_account_ref":"acct-1","ratelimit_headers":{"anthropic-ratelimit-unified-5h-utilization":"0.2","anthropic-ratelimit-unified-status":"allowed"},"sent_at_ms":1790000000000,"ttl_ms":3600000,"expires_at_ms":1790003600000}"#,
    r#"{"type":"cache_warmed","run_id":"r","step_id":2,"request_body_sha256":"aa","warm_body_sha256":"bb","usage":{},"sent_at_ms":1790000000000}"#,
];

const INPUT_CAP_PINNED: &[&str] = &[
    r#"{"type":"input_token_cap_frozen","limit":150}"#,
    r#"{"type":"input_token_budget_exhausted","detail":{"limit":150,"spent":150,"cause":"limit_reached"}}"#,
    r#"{"type":"input_token_budget_exhausted","detail":{"limit":150,"spent":100,"cause":"usage_unreported"}}"#,
];

#[test]
fn input_cap_records_are_must_understand_and_round_trip() {
    use crate::framing::{decode_next, encode, DecodeStep, INPUT_TOKEN_CAP_FEATURE};
    for bytes in INPUT_CAP_PINNED {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), *bytes);
        assert_eq!(record.required_features(), &[INPUT_TOKEN_CAP_FEATURE]);
        let DecodeStep::Record { framed, .. } = decode_next(&encode(7, 3, &record).unwrap()) else {
            panic!("cap record must decode")
        };
        assert_eq!(framed.record, record);
    }
    let plain: WalRecord = serde_json::from_str(PINNED[1]).unwrap();
    let mut capped = plain.clone();
    let WalRecord::RunStarted { config, .. } = &mut capped else {
        unreachable!()
    };
    config.budget = Some(BudgetConfig {
        max_input_tokens: Some(150),
        ..Default::default()
    });
    assert_eq!(capped.required_features(), &[INPUT_TOKEN_CAP_FEATURE]);
    if let Some(path) = std::env::var_os("BROCA_INPUT_CAP_WAL_FIXTURE") {
        let mut bytes = encode(1, 3, &capped).unwrap();
        bytes.extend(encode(2, 3, &WalRecord::InputTokenCapFrozen { limit: 150 }).unwrap());
        std::fs::write(&path, bytes).unwrap();
        let mut plain_path = path;
        plain_path.push(".plain");
        std::fs::write(plain_path, encode(1, 3, &plain).unwrap()).unwrap();
    }
}

/// Records of tool calls held for a human's approval. Kept apart from `PINNED`, which holds the
/// kinds and fields that existed before compaction and is checked against those builds' frame
/// requirements. The full fixtures of these records, framed and gated under `deferred-call/v1`,
/// are in `framing/deferred_call_tests.rs`; these pin the record bytes beside every other kind's.
const DEFERRED_CALL_PINNED: &[&str] = &[
    r#"{"type":"tool_call_deferred","batch_id":"r-b1","call_id":"t1","executor":"broca","tool":"edit","args_jcs":"{\"f\":\"a\"}","args_hash":"910b42ae2f4a973d3af5f056c18c62742efb4f7c4af344444e47e071411d6e57","scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3},"ask":{"prompt":"Edit a?","expires_at_ms":1790000723000,"on_expiry":"deny","material_damage":false,"late_execution":"notify_only"}}"#,
    r#"{"type":"approval_recorded","call_key":"lineage:7","call_id":"t1","elicitation_id":"el-1","decision":"approved","approval_digest":"sha256:ab12"}"#,
    r#"{"type":"awaiting_approval","batch_id":"r-b1","pending":[]}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","args":{"f":"a"},"approval_digest":"sha256:ab12","call_key":"lineage:7"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool not run: batch interrupted"},"is_error":true},"outcome":"interrupted","withdraw_elicitation":{"call_key":"lineage:7"}}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"decision pending: el-2"},"is_error":false},"outcome":"completed","provider_held":{"call_key":"lineage:9","module":"plexus","elicitation_id":"el-2"}}"#,
    r#"{"type":"late_approval_recorded","call_key":"lineage:7","elicitation_id":"el-1","decision":"none"}"#,
    r#"{"type":"late_dispatch_intent","call_key":"lineage:7","approval_digest":"sha256:ab12","batch_id":"late:lineage:7"}"#,
    r#"{"type":"late_result","call_key":"lineage:7","outcome":"unknown","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool outcome unknown"},"is_error":true}}"#,
    r#"{"type":"question_withdrawal_acknowledged","call_key":"lineage:7","answer":"not_sent","reason":"expired"}"#,
];

const CANCEL_AT_PINNED: &[&str] = &[
    r#"{"type":"run_cancel_requested","run_id":"r","at":"step_boundary"}"#,
    r#"{"type":"run_cancel_requested","run_id":"r","at":"now"}"#,
];

#[test]
fn cancellation_records_are_must_understand_and_round_trip() {
    use crate::framing::{decode_next, encode, DecodeStep, RUN_CANCEL_AT_FEATURE};
    for bytes in CANCEL_AT_PINNED {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), *bytes);
        assert_eq!(record.required_features(), &[RUN_CANCEL_AT_FEATURE]);
        let frame = encode(7, 3, &record).unwrap();
        let DecodeStep::Record { framed, .. } = decode_next(&frame) else {
            panic!("reader must know cancel-at")
        };
        assert_eq!(framed.record, record);
        if bytes.contains("step_boundary") {
            if let Some(path) = std::env::var_os("BROCA_CANCEL_AT_WAL_FIXTURE") {
                let start: WalRecord = serde_json::from_str(r#"{"type":"run_started","run_id":"r","session":{"project_root":"/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}}"#).unwrap();
                let mut affected = encode(1, 3, &start).unwrap();
                affected
                    .extend(encode(2, 3, &WalRecord::StepStarted { step_id: StepId(1) }).unwrap());
                let mut plain = affected.clone();
                affected.extend(encode(3, 3, &record).unwrap());
                std::fs::write(&path, affected).unwrap();
                let step: WalRecord = serde_json::from_str(r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[{"type":"text","text":"done"}]},"usage":{},"retries_used":0}"#).unwrap();
                plain.extend(encode(3, 3, &step).unwrap());
                let terminal: WalRecord = serde_json::from_str(
                    r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
                )
                .unwrap();
                plain.extend(encode(4, 3, &terminal).unwrap());
                let mut plain_path = path;
                plain_path.push(".plain");
                std::fs::write(plain_path, plain).unwrap();
            }
        }
    }
}

/// The pre-compaction byte pins stay separate from new record fixtures. In addition to the
/// record JSON, pin the feature envelope: changing it would make a rollback read different
/// bytes or refuse a session it previously understood.
#[test]
fn pre_compaction_records_keep_their_bytes_and_frame_requirements() {
    use crate::framing::{encode, FRAMING_VERSION, GATED_FRAMING_VERSION, HEADER_LEN};

    let additional = include_str!("../../tests/fixtures/pre_compaction_records.jsonl");
    for pinned in PINNED.iter().copied().chain(additional.lines()) {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        let expected_features: &[&str] = match &record {
            WalRecord::Steered { .. } => &["steer-queue/v1"],
            WalRecord::RunPaused {
                reason: PauseReason::Restart,
                ..
            } => &["restart-pause/v1"],
            WalRecord::ToolDispatchIntent {
                module_id: Some(_), ..
            } => &["dispatch-module/v1"],
            WalRecord::RunStarted { scope: Some(_), .. } => &["scope/v1"],
            WalRecord::RunStarted {
                manifest: Some(_), ..
            } => &["plan-manifest/v1"],
            _ => &[],
        };
        assert_eq!(record.required_features(), expected_features, "{pinned}");
        let frame = encode(7, 3, &record).unwrap();
        let (version, payload) = if expected_features.is_empty() {
            (FRAMING_VERSION, pinned.to_owned())
        } else {
            (
                GATED_FRAMING_VERSION,
                format!(
                    r#"{{"requires":{},"record":{pinned}}}"#,
                    serde_json::to_string(expected_features).unwrap()
                ),
            )
        };
        assert_eq!(frame[4], version);
        assert_eq!(&frame[HEADER_LEN..], payload.as_bytes());
    }
}

#[test]
fn every_record_kind_keeps_its_pinned_bytes() {
    for pinned in PINNED
        .iter()
        .chain(DEFERRED_CALL_PINNED)
        .chain(CANCEL_AT_PINNED)
        .chain(INPUT_CAP_PINNED)
    {
        let decoded: WalRecord = serde_json::from_str(pinned)
            .unwrap_or_else(|error| panic!("pinned record must decode: {error}\n{pinned}"));
        assert_ne!(
            decoded,
            WalRecord::Unknown,
            "a pinned record decoded as an unknown kind: {pinned}"
        );
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
            *pinned,
            "a pinned record must re-encode to exactly the bytes it was read from"
        );
    }
}

/// A resume record written by a later build stays readable here: a count this build does not
/// know is ignored, and a redo kind it does not know decodes as `ResumeRedo::Unknown` instead of
/// failing the whole WAL as corrupt. The record is skippable, but only if it decodes at all.
#[test]
fn a_resume_record_from_a_later_build_still_decodes() {
    let later = r#"{"type":"resume_started","episode":"r","resumed_from_seq":9,"replayed":{"messages":2,"hook_outputs":4},"redone":[{"kind":"hook_run","hook":"pre_tool"}],"indeterminate":["t1"],"note":"x"}"#;
    let decoded: WalRecord = serde_json::from_str(later).expect("a later resume record decodes");
    assert!(decoded.is_informational());
    let WalRecord::ResumeStarted {
        replayed, redone, ..
    } = decoded
    else {
        panic!("expected ResumeStarted, got {decoded:?}");
    };
    assert_eq!(
        replayed,
        ResumeReplayed {
            messages: 2,
            model_steps: 0,
            tool_results: 0
        }
    );
    assert_eq!(redone, vec![ResumeRedo::Unknown]);
}

/// The pinned list covers every kind this build can write, so no kind's bytes can drift
/// without a test failing. `Unknown`, the placeholder for a kind this build does not know, is
/// never written.
#[test]
fn the_pinned_bytes_cover_every_record_kind() {
    let pinned: std::collections::BTreeSet<&str> = PINNED
        .iter()
        .copied()
        .chain(include_str!("../../tests/fixtures/compaction_records.jsonl").lines())
        .chain(DEFERRED_CALL_PINNED.iter().copied())
        .chain(CANCEL_AT_PINNED.iter().copied())
        .chain(INPUT_CAP_PINNED.iter().copied())
        .map(|bytes| tag_of(&serde_json::from_str::<WalRecord>(bytes).unwrap()))
        .collect();
    // The tag of every arm of the exhaustive `tag_of` except `WalRecord::Unknown`, the
    // placeholder for a kind this build does not know, which is never written. A new variant stops `tag_of`
    // compiling; whoever adds its arm must add its tag here too, and this assertion then fails
    // until `PINNED` holds bytes for it.
    let every_kind = [
        "run_started",
        "model_step_finished",
        "model_attempt_finished",
        "step_started",
        "tool_batch_started",
        "tool_dispatch_intent",
        "tool_no_dispatch",
        "tool_result",
        "tool_batch_finished",
        "turn_finished",
        "budget_fired",
        "run_finished",
        "run_cancel_requested",
        "input_token_cap_frozen",
        "input_token_budget_exhausted",
        "run_paused",
        "run_config_refrozen",
        "run_auth_refrozen",
        "queued",
        "steered",
        "retracted",
        "approval_requested",
        "approval_resolved",
        "resume_started",
        "cache_warmed",
        "compaction_request",
        "compaction_message",
        "compaction_answer",
        "compaction_wait",
        "tool_call_deferred",
        "approval_recorded",
        "awaiting_approval",
        "late_approval_recorded",
        "late_dispatch_intent",
        "late_result",
        "question_withdrawal_acknowledged",
    ];
    assert_eq!(pinned, every_kind.into_iter().collect());
    assert_eq!(tag_of(&WalRecord::Unknown), "unknown");
}

/// A cache warm record is informational (replay, projection and the control stream skip it) and
/// needs no reader feature, so an older build that does not know the kind skips it rather than
/// refusing the WAL.
#[test]
fn a_cache_warm_record_is_informational_and_needs_no_reader_feature() {
    let warm: WalRecord = serde_json::from_str(PINNED[PINNED.len() - 1]).unwrap();
    assert!(matches!(warm, WalRecord::CacheWarmed { .. }));
    assert!(warm.is_informational());
    assert!(warm.required_features().is_empty());
}

/// An attempt's send time is an optional field like the attempt's others: a record without it
/// (every record written before it existed) reads as `None`, and a record with it needs no reader
/// feature, so an older build reads the record and ignores the field.
#[test]
fn an_attempt_send_time_is_optional_and_needs_no_reader_feature() {
    let send_time = |pinned: &str| match serde_json::from_str::<WalRecord>(pinned).unwrap() {
        record @ WalRecord::ModelAttemptFinished { .. } => {
            assert!(record.required_features().is_empty());
            let WalRecord::ModelAttemptFinished { sent_at_ms, .. } = record else {
                unreachable!()
            };
            sent_at_ms
        }
        other => panic!("expected an attempt record, got {other:?}"),
    };
    let attempts: Vec<&str> = PINNED
        .iter()
        .copied()
        .filter(|pinned| pinned.starts_with(r#"{"type":"model_attempt_finished""#))
        .collect();
    let sent: Vec<Option<u64>> = attempts.iter().map(|pinned| send_time(pinned)).collect();
    // Exactly one pinned attempt carries a send time; every other one was written without it.
    assert_eq!(
        sent.iter().filter(|sent| sent.is_some()).count(),
        1,
        "{sent:?}"
    );
    assert!(sent.contains(&Some(1_789_999_940_000)), "{sent:?}");
    assert!(
        sent.len() > 1,
        "the absent case must be pinned too: {sent:?}"
    );
}

/// These literal write-ahead log records, written before the tool-wait keep-warm scheduler
/// existed, must still read and serialize unchanged, without new optional metadata. They are
/// separate from the broader record table so later edits to that table cannot replace them.
#[test]
fn pre_keep_warm_records_keep_their_original_bytes() {
    let original = [
        r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
        r#"{"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#,
        r#"{"type":"cache_warmed","run_id":"r","step_id":2,"request_body_sha256":"aa","warm_body_sha256":"bb","usage":{},"sent_at_ms":1790000000000}"#,
    ];
    for bytes in original {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert!(matches!(
            record,
            WalRecord::RunStarted { .. } | WalRecord::Queued { .. } | WalRecord::CacheWarmed { .. }
        ));
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }
}

#[test]
fn keep_warm_record_metadata_matches_goldens() {
    use broca_protocol::request::KeepWarm;
    let bases = [
        (
            PINNED[1],
            include_str!("../../tests/golden/run_started.keep_warm.json"),
        ),
        (
            PINNED[40],
            include_str!("../../tests/golden/queued.keep_warm.json"),
        ),
        (
            PINNED[PINNED.len() - 1],
            include_str!("../../tests/golden/cache_warmed.keep_warm.json"),
        ),
    ];
    for (base, golden) in bases {
        let mut record: WalRecord = serde_json::from_str(base).unwrap();
        match &mut record {
            WalRecord::RunStarted { keep_warm, .. } => *keep_warm = Some(KeepWarm::Auto),
            WalRecord::Queued {
                keep_warm, request, ..
            } => {
                *keep_warm = Some(KeepWarm::Auto);
                request.keep_warm = Some(KeepWarm::Auto);
            }
            WalRecord::CacheWarmed { trigger, .. } => *trigger = Some("keep_warm".into()),
            _ => panic!("expected run, queue or warm"),
        }
        assert_eq!(
            format!("{}\n", serde_json::to_string(&record).unwrap()),
            golden
        );
        assert_eq!(serde_json::from_str::<WalRecord>(golden).unwrap(), record);
    }
}

/// Transport evidence is informational: old bytes remain unchanged, and a future pipe's
/// name remains readable without turning an otherwise valid WAL into corruption.
#[test]
fn attempt_transport_is_optional_and_needs_no_reader_feature() {
    let old = r#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":0,"response_body_sha256":"e3b0","outcome":"connect_failed","ts_ms":5}"#;
    let record: WalRecord = serde_json::from_str(old).unwrap();
    assert!(matches!(
        record,
        WalRecord::ModelAttemptFinished {
            transport: None,
            websocket_request: None,
            websocket_close_code: None,
            ..
        }
    ));
    assert_eq!(serde_json::to_string(&record).unwrap(), old);
    for transport in ["websocket", "https", "future-pipe"] {
        let mut value: serde_json::Value = serde_json::from_str(old).unwrap();
        value["transport"] = transport.into();
        let record: WalRecord = serde_json::from_value(value).unwrap();
        assert!(record.required_features().is_empty());
        assert!(record.is_informational());
        assert!(
            matches!(record, WalRecord::ModelAttemptFinished { transport: Some(ref actual), .. } if actual == transport)
        );
    }
}

/// These literal records, written before a `session.send` could carry a title, must still read
/// and serialize unchanged, and read with no title. Kept apart from the broader table above so a
/// later edit there cannot replace them.
#[test]
fn pre_title_records_keep_their_original_bytes() {
    let original = [
        r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"direct"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-7"}"#,
        r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
        r#"{"type":"queued","submission_id":"sub-run-8","request":{"project_id":"pj-a","model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","principal":{"kind":"direct"}}"#,
        r#"{"type":"queued","submission_id":"sub-run-9","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"stop and do this"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"send_id":"interrupt-1","delivery":"interrupt"}"#,
        r#"{"type":"steered","send_id":"steer-1","request_identity_digest":"v1:abc","delivery":"steer","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
    ];
    for bytes in original {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert!(matches!(
            record,
            WalRecord::RunStarted { .. } | WalRecord::Queued { .. } | WalRecord::Steered { .. }
        ));
        assert_eq!(record.title(), None, "an old record reads with no title");
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }
}

/// A title is an ordinary optional field on each record a send writes: it is written where the
/// struct declares it, reads back as written, and needs no reader feature beyond what the record
/// already needed (a steer keeps its own feature; a run start or queued send still needs none),
/// so an older build reads the record and ignores the title.
#[test]
fn a_title_is_optional_and_needs_no_reader_feature() {
    let titled = [
        (
            PINNED[1],
            r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"title":"Fix the login bug"}"#,
        ),
        (
            r#"{"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#,
            r#"{"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"title":"Fix the login bug"}"#,
        ),
        (
            r#"{"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
            r#"{"type":"steered","title":"Fix the login bug","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
        ),
    ];
    for (untitled, expected) in titled {
        let base: WalRecord = serde_json::from_str(untitled).unwrap();
        let mut record = base.clone();
        match &mut record {
            WalRecord::RunStarted { title, .. }
            | WalRecord::Queued { title, .. }
            | WalRecord::Steered { title, .. } => *title = Some("Fix the login bug".into()),
            other => panic!("expected a record a send writes, got {other:?}"),
        }
        assert_eq!(serde_json::to_string(&record).unwrap(), expected);
        let decoded: WalRecord = serde_json::from_str(expected).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(decoded.title(), Some("Fix the login bug"));
        assert_eq!(record.required_features(), base.required_features());
    }
}

/// Existing ordinary and flow scopes have no agent-run identifier. Adding optional `run_id`
/// and `agent_id` must leave their record JSON and reader feature requirements unchanged, so
/// a rolled-back build can still read new episodes of these sessions.
#[test]
fn pre_agent_run_scopes_keep_their_bytes_and_frame_requirements() {
    use crate::framing::{encode, GATED_FRAMING_VERSION, HEADER_LEN};
    const SCOPES: &[(&str, &[&str])] = &[
        (
            r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3}"#,
            &["scope/v1"],
        ),
        (
            r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"flow-1","scope_epoch":3,"flow_id":"flow-1","sender":{"kind":"reserved","module_id":"basal"}}"#,
            &["flow-scopes/v1", "scope/v1"],
        ),
    ];
    for (scope, features) in SCOPES {
        let pinned = format!("{},\"scope\":{scope}}}", &PINNED[1][..PINNED[1].len() - 1]);
        let record: WalRecord = serde_json::from_str(&pinned).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        assert_eq!(record.required_features(), *features);
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], GATED_FRAMING_VERSION);
        let payload = format!(
            r#"{{"requires":{},"record":{pinned}}}"#,
            serde_json::to_string(features).unwrap()
        );
        assert_eq!(&frame[HEADER_LEN..], payload.as_bytes());
    }
}
