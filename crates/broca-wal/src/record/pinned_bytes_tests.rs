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

/// Messages written before durable envelope times retain their original payload and framing.
#[test]
fn pre_message_times_records_keep_exact_bytes() {
    use crate::framing::{
        encode, FRAMING_VERSION, GATED_FRAMING_VERSION, HEADER_LEN, STEER_QUEUE_FEATURE,
    };
    for pinned in include_str!("../../tests/fixtures/before_message_times.jsonl").lines() {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        let frame = encode(7, 3, &record).unwrap();
        if matches!(record, WalRecord::Steered { .. }) {
            assert_eq!(record.required_features(), &[STEER_QUEUE_FEATURE]);
            assert_eq!(frame[4], GATED_FRAMING_VERSION);
            assert_eq!(
                &frame[HEADER_LEN..],
                format!(r#"{{"requires":["steer-queue/v1"],"record":{pinned}}}"#).as_bytes()
            );
        } else {
            assert!(record.required_features().is_empty());
            assert_eq!(frame[4], FRAMING_VERSION);
            assert_eq!(&frame[HEADER_LEN..], pinned.as_bytes());
        }
    }
}

/// Pre-interrupt `run_finished` records keep their exact bytes and require no reader feature.
#[test]
fn pre_interrupt_reason_terminal_bytes_and_features_are_pinned() {
    use crate::framing::{encode, FRAMING_VERSION, HEADER_LEN};

    for pinned in [
        r#"{"type":"run_finished","reason":"interrupted","usage":{},"retries_used":0}"#,
        r#"{"type":"run_finished","reason":"interrupted","ts_ms":42,"usage":{},"retries_used":0}"#,
    ] {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        assert!(record.required_features().is_empty());
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], FRAMING_VERSION);
        assert_eq!(&frame[HEADER_LEN..], pinned.as_bytes());
    }
}

/// Existing reasoning tags and scalar frozen flags predate the OpenRouter continuation format.
/// Their payload bytes and ungated frame version must survive adding a reader feature.
#[test]
fn pre_openrouter_reasoning_records_keep_their_bytes_and_frame_requirements() {
    use crate::framing::{encode, FRAMING_VERSION, HEADER_LEN};
    for pinned in include_str!("../../tests/fixtures/before_openrouter_reasoning.jsonl").lines() {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        assert!(record.required_features().is_empty());
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], FRAMING_VERSION);
        assert_eq!(&frame[HEADER_LEN..], pinned.as_bytes());
    }
}

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

/// Credential selections as every build up to now writes them, with every optional member
/// present and with none. A selection that names how its credential authenticates is a later,
/// gated addition; these shapes must keep their exact bytes and stay readable by any build, so
/// they require no reader feature and keep the ungated frame version.
#[test]
fn auth_selections_without_a_frozen_method_keep_their_bytes_and_frame_requirements() {
    use crate::framing::{encode, FRAMING_VERSION, HEADER_LEN};

    for pinned in [
        r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"anthropic","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","resolved_credential_id":"oauth:anthropic:work","vault_request":{"min_ttl_ms":600000,"op":"get_scoped"},"account_decision":{"conversation_key":"broca-session:abc","conversation_key_source":"session","path":"routed","admission_id":"r","decision_id":"acd-1"}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
        r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"deepseek","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"apikey:deepseek"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
        r#"{"type":"run_auth_refrozen","ts_ms":5,"auth_selection":{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","resolved_credential_id":"oauth:anthropic:shared","vault_request":{"min_ttl_ms":600000,"op":"get_scoped"},"account_decision":{"path":"pinned","admission_id":"r"}}}"#,
        r#"{"type":"run_auth_refrozen","auth_selection":{"credential_id":"oauth:anthropic:work"}}"#,
    ] {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        assert!(record.required_features().is_empty(), "{pinned}");
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
        WalRecord::WriterOpened { .. } => "writer_opened",
        WalRecord::WriterClosed { .. } => "writer_closed",
        WalRecord::VisibilityLate { .. } => "visibility_late",
        WalRecord::RunStarted { .. } => "run_started",
        WalRecord::ModelStepFinished { .. } => "model_step_finished",
        WalRecord::ModelStepRefused { .. } => "model_step_refused",
        WalRecord::ModelAttemptFinished { .. } => "model_attempt_finished",
        WalRecord::AttemptReserved { .. } => "attempt_reserved",
        WalRecord::AttemptFinished { .. } => "attempt_finished",
        WalRecord::AttemptReleased { .. } => "attempt_released",
        WalRecord::AttemptCorrected { .. } => "attempt_corrected",
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
        WalRecord::CompactionViewApplied { .. } => "compaction_view_applied",
        WalRecord::CompactionNotice { .. } => "compaction_notice",
        WalRecord::ToolCallDeferred { .. } => "tool_call_deferred",
        WalRecord::ApprovalRecorded { .. } => "approval_recorded",
        WalRecord::AwaitingApproval { .. } => "awaiting_approval",
        WalRecord::LateApprovalRecorded { .. } => "late_approval_recorded",
        WalRecord::LateDispatchIntent { .. } => "late_dispatch_intent",
        WalRecord::LateResult { .. } => "late_result",
        WalRecord::QuestionWithdrawalAcknowledged { .. } => "question_withdrawal_acknowledged",
        WalRecord::ToolCallProviderHeld { .. } => "tool_call_provider_held",
        WalRecord::InputTokenCapFrozen { .. } => "input_token_cap_frozen",
        WalRecord::InputTokenBudgetExhausted { .. } => "input_token_budget_exhausted",
        WalRecord::ContextCeilingWait { .. } => "context_ceiling_wait",
        WalRecord::ContextCeilingDecided { .. } => "context_ceiling_decided",
        WalRecord::OutputReservationClamped { .. } => "output_reservation_clamped",
        WalRecord::CompactionRideWait { .. } => "compaction_ride_wait",
        WalRecord::CompactionRideWaitEnded { .. } => "compaction_ride_wait_ended",
        WalRecord::StructuredOutputResult { .. } => "structured_output_result",
        WalRecord::StructuredOutputDowngraded { .. } => "structured_output_downgraded",
        WalRecord::DeliveryOrdered { .. } => "delivery_ordered",
        WalRecord::SendEnded { .. } => "send_ended",
        WalRecord::SendDelivered { .. } => "send_delivered",
        WalRecord::Unknown => "unknown",
    }
}

/// Every record kind this build writes, each with its optional fields both present and absent
/// where it has any. `Unknown` is absent because it can never be written.
const WRITER_ID_PINNED: &[&str] = &[
    r#"{"type":"writer_opened","writer_id":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"recovered_through_seq":0,"cause":"new"}"#,
    r#"{"type":"writer_opened","writer_id":[2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2],"previous_writer_id":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"recovered_through_seq":5,"cause":"reopen"}"#,
    r#"{"type":"writer_closed","writer_id":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]}"#,
];

#[test]
fn writer_id_records_keep_their_bytes_and_require_writer_id() {
    for bytes in WRITER_ID_PINNED {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(record.required_features(), ["writer-id/v1"]);
        assert_eq!(serde_json::to_string(&record).unwrap(), *bytes);
    }
}

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

const OUTPUT_RESERVATION_PINNED: &[&str] = &[
    r#"{"type":"output_reservation_clamped","step_id":175,"max_output_tokens":124990,"estimated_input_tokens":919490,"context_limit":1048576}"#,
];

/// A lowered output cap is written in a gated frame requiring `output-reservation/v1`: a build
/// that does not know the kind must refuse the WAL rather than resume the step with the frozen
/// cap, which would be a different request from the one recorded.
#[test]
fn an_output_reservation_record_keeps_its_bytes_and_requires_its_feature() {
    use crate::framing::{encode, GATED_FRAMING_VERSION, HEADER_LEN, OUTPUT_RESERVATION_FEATURE};
    for pinned in OUTPUT_RESERVATION_PINNED {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert!(matches!(
            record,
            WalRecord::OutputReservationClamped {
                max_output_tokens: 124_990,
                ..
            }
        ));
        assert_eq!(serde_json::to_string(&record).unwrap(), *pinned);
        assert_eq!(record.required_features(), &[OUTPUT_RESERVATION_FEATURE]);
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], GATED_FRAMING_VERSION);
        assert_eq!(
            &frame[HEADER_LEN..],
            format!(r#"{{"requires":["output-reservation/v1"],"record":{pinned}}}"#).as_bytes()
        );
    }
}

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

/// The records of sends with an ordered `delivery` list, each with its optional fields present
/// and absent.
const DELIVERY_ORDER_PINNED: &[&str] = &[
    r#"{"type":"delivery_ordered","submission_id":"sub-r2","order":{"modes":["tool_result","turn_system","steer","idle_start"],"dedupe_key":"ctx-reduce","expires_at_ms":1790000000000},"sent_by":{"kind":"reserved","module_id":"ck-mc"}}"#,
    r#"{"type":"delivery_ordered","submission_id":"sub-r2","order":{"modes":["steer"]}}"#,
    r#"{"type":"send_ended","submission_id":"sub-r2","reason":{"kind":"superseded","by":"sub-r3"}}"#,
    r#"{"type":"send_ended","submission_id":"sub-r2","reason":{"kind":"expired"}}"#,
    r#"{"type":"send_delivered","submission_id":"sub-r2","run_id":"r1","mode":"steer","input":[{"role":"user","content":[{"type":"text","text":"note"}]}]}"#,
    r#"{"type":"send_delivered","submission_id":"sub-r2","run_id":"r1","mode":"tool_result","tool_call_id":"t1","appended_text":"\n\nnote"}"#,
];

/// Every delivery-order record is must-understand: it is written only in a gated frame naming
/// `delivery-order/v1` (a build without that name refuses it; see the framing tests), and this
/// build reads it back unchanged.
#[test]
fn delivery_order_records_are_must_understand_and_round_trip() {
    use crate::framing::{
        decode_next, encode, DecodeStep, DELIVERY_ORDER_FEATURE, GATED_FRAMING_VERSION,
    };
    for bytes in DELIVERY_ORDER_PINNED {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), *bytes);
        assert!(!record.is_informational(), "{bytes}");
        assert_eq!(record.required_features(), &[DELIVERY_ORDER_FEATURE]);
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], GATED_FRAMING_VERSION);
        let DecodeStep::Record { framed, .. } = decode_next(&frame) else {
            panic!("this build must read {bytes}")
        };
        assert_eq!(framed.record, record);
    }
}

const CANCEL_AT_PINNED: &[&str] = &[
    r#"{"type":"run_cancel_requested","run_id":"r","at":"step_boundary"}"#,
    r#"{"type":"run_cancel_requested","run_id":"r","at":"now"}"#,
];

/// The two values `provider-held/v1` covers: a call its tool provider holds past its reply,
/// with and without a held detail, and the pause of a run waiting on such a call.
const PROVIDER_HELD_PINNED: &[&str] = &[
    r#"{"type":"tool_call_provider_held","batch_id":"r-b1","tool_call_id":"t1","call_key":"lin_9:42","module":"core","reason":"parent_answer","detail_digest":"9f2a","held_at_ms":1761220000000,"sequential":true}"#,
    r#"{"type":"tool_call_provider_held","batch_id":"r-b1","tool_call_id":"t1","call_key":"lin_9:42","module":"core","reason":"running","held_at_ms":1761220000000,"sequential":false}"#,
    r#"{"type":"run_paused","run_id":"r1","ts_ms":1761220000001,"reason":"tool_held"}"#,
    r#"{"type":"run_paused","run_id":"r1","reason":"tool_held"}"#,
];

/// Each provider-held value keeps its bytes and is written only in a gated frame whose payload
/// is exactly `{"requires":["provider-held/v1"],"record":...}`. The requirement comes from the
/// record itself, so no writer can put either value in a frame an older build reads unchecked.
/// This build lists the feature, so it reads such a frame back unchanged; a reader without the
/// name refuses it as a newer writer's (`framing::provider_held_tests`).
#[test]
fn provider_held_records_keep_their_bytes_and_require_only_provider_held() {
    use crate::framing::{decode_next, encode, DecodeStep, GATED_FRAMING_VERSION, HEADER_LEN};
    for pinned in PROVIDER_HELD_PINNED {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), *pinned);
        assert!(!record.is_informational(), "{pinned}");
        assert_eq!(
            record.required_features(),
            &[crate::framing::PROVIDER_HELD_FEATURE],
            "{pinned}"
        );
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], GATED_FRAMING_VERSION, "{pinned}");
        assert_eq!(
            &frame[HEADER_LEN..],
            format!(r#"{{"requires":["provider-held/v1"],"record":{pinned}}}"#).as_bytes()
        );
        match decode_next(&frame) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record, "{pinned}"),
            _ => panic!("this build reads its own provider-held frame: {pinned}"),
        }
    }
}

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

/// The content-filter fallback records and fields, each with its optional members present and
/// absent: a refused step, and a step carrying the handoff part and each new step field. All
/// optional members are absent by default, so the ordinary steps in `PINNED` keep their bytes.
const FALLBACK_PINNED: &[(&str, &[&str])] = &[
    (
        r#"{"type":"model_step_refused","step_id":4,"usage":{"input_tokens":120,"cached_input_tokens":4000,"output_tokens":3},"served_model":"claude-opus-4-8","discarded_blocks":2,"schedule":{"source":{"provider_module_id":"anthropic","model_id":"claude-opus-5-5"},"fallback":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"phase":{"kind":"fallback","remaining":20},"re_refusals":1,"updated_at_ms":1790000000000},"ts_ms":1790000000000}"#,
        &["refusal-fallback/v1"],
    ),
    (
        r#"{"type":"model_step_refused","step_id":4,"usage":{},"discarded_blocks":0,"schedule":{"source":{"provider_module_id":"anthropic","model_id":"claude-opus-5-5"},"fallback":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"phase":{"kind":"probe","clean":3},"re_refusals":0}}"#,
        &["refusal-fallback/v1"],
    ),
    (
        r#"{"type":"model_step_finished","step_id":4,"assistant_message":{"message_id":"msg-4","content":[{"type":"text","text":"Let me"},{"type":"provider_fallback","from_model":"claude-opus-5-5","to_model":"claude-opus-4-8"},{"type":"text","text":" check."}]},"usage":{"input_tokens":3,"output_tokens":9},"finish_reason":"stop","retries_used":0,"served_model":"claude-opus-4-8","usage_iterations":[{"type":"message","model":"claude-opus-5-5","input_tokens":3,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":4000},{"type":"fallback_message","model":"claude-opus-4-8","output_tokens":9,"cache_creation":{"ephemeral_1h_input_tokens":0}}]}"#,
        &["provider-fallback-block/v1"],
    ),
    (
        r#"{"type":"model_step_finished","step_id":5,"assistant_message":{"message_id":"msg-5","content":[{"type":"tool_call","tool_call_id":"t1","tool_name":"edit","input":{"f":"a"}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0,"refusal_fallback":{"source":{"provider_module_id":"anthropic","model_id":"claude-opus-5-5"},"fallback":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"phase":{"kind":"fallback","remaining":10},"re_refusals":0},"refusal_after_tool_use":true}"#,
        &["refusal-fallback/v1"],
    ),
    (
        r#"{"type":"model_step_finished","step_id":6,"assistant_message":{"message_id":"msg-6","content":[]},"usage":{},"retries_used":1,"route":{"model":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"reason":"schedule"},"refusal_fallback":{"source":{"provider_module_id":"anthropic","model_id":"claude-opus-5-5"},"fallback":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"phase":{"kind":"fallback","remaining":9},"re_refusals":0,"updated_at_ms":5}}"#,
        &["refusal-fallback/v1"],
    ),
    (
        r#"{"type":"model_step_finished","step_id":7,"assistant_message":{"message_id":"msg-7","content":[]},"usage":{},"retries_used":0,"served_model":"claude-opus-4-8","route":{"model":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"reason":"refusal"}}"#,
        &["provider-fallback-block/v1", "refusal-fallback/v1"],
    ),
];

/// Each fallback record keeps its bytes, requires exactly the features listed beside it, is
/// written in a gated frame naming them, and reads back unchanged.
#[test]
fn fallback_records_keep_their_bytes_and_require_exactly_their_features() {
    use crate::framing::{decode_next, encode, DecodeStep, GATED_FRAMING_VERSION, HEADER_LEN};
    for (pinned, features) in FALLBACK_PINNED {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), *pinned);
        assert!(!record.is_informational(), "{pinned}");
        assert_eq!(record.required_features(), *features, "{pinned}");
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], GATED_FRAMING_VERSION, "{pinned}");
        assert_eq!(
            &frame[HEADER_LEN..],
            format!(
                r#"{{"requires":{},"record":{pinned}}}"#,
                serde_json::to_string(features).unwrap()
            )
            .as_bytes()
        );
        match decode_next(&frame) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record, "{pinned}"),
            _ => panic!("this build reads its own fallback frame: {pinned}"),
        }
    }
}

/// A step written before the fallback fields existed decodes with every one of them absent,
/// and a step built with them absent writes exactly the old bytes.
#[test]
fn a_step_without_fallback_fields_reads_them_absent_and_writes_no_bytes_for_them() {
    for pinned in PINNED
        .iter()
        .filter(|pinned| pinned.starts_with(r#"{"type":"model_step_finished""#))
    {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        let WalRecord::ModelStepFinished { fallback, .. } = &record else {
            unreachable!()
        };
        assert_eq!(**fallback, StepFallback::default(), "{pinned}");
        assert!(record.required_features().is_empty(), "{pinned}");
        assert_eq!(serde_json::to_string(&record).unwrap(), *pinned);
    }
}

#[test]
fn every_record_kind_keeps_its_pinned_bytes() {
    for pinned in PINNED
        .iter()
        .chain(DEFERRED_CALL_PINNED)
        .chain(CANCEL_AT_PINNED)
        .chain(PROVIDER_HELD_PINNED)
        .chain(DELIVERY_ORDER_PINNED)
        .chain(INPUT_CAP_PINNED)
        .chain(STRUCTURED_OUTPUT_PINNED)
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

/// Asynchronous compaction's records and the Setup record carrying a ceiling: their bytes, and
/// that each is written in a gated frame naming `compaction-view/v1`, so a build that does not
/// know the feature refuses the WAL instead of rendering a resumed request without its view.
const COMPACTION_VIEW_PINNED: &[&str] = &[
    r#"{"type":"compaction_view_applied","provider":"mc","view_version":2,"view":"{\"session_ref\":\"0123456789abcdef0123456789abcdef\",\"view_version\":2,\"lineage_id\":\"l\",\"covers_through\":{\"ordinal\":1,\"mid\":\"m1\"},\"ranges\":[{\"kind\":\"replace\",\"from\":0,\"to\":1,\"bytes\":\"[]\"},{\"kind\":\"keep\",\"from\":1,\"to\":2}],\"digest\":\"00\"}","choice":{"kind":"applied"}}"#,
    r#"{"type":"compaction_view_applied","provider":"mc","view_version":3,"view":"{\"session_ref\":\"0123456789abcdef0123456789abcdef\",\"view_version\":3,\"lineage_id\":\"l\",\"covers_through\":{\"ordinal\":0,\"mid\":\"m0\"},\"ranges\":[{\"kind\":\"replace\",\"from\":0,\"to\":0,\"bytes\":\"[]\"},{\"kind\":\"keep\",\"from\":0,\"to\":1}],\"digest\":\"00\"}","choice":{"kind":"companion","edit":"strip_signed_reasoning","strip":{"from_ordinal":0,"to_ordinal":4}}}"#,
    r#"{"type":"compaction_notice","seq":1,"headers":{"est_tokens":"10","lineage_id":"l","model":"m","seq":"1","session_ref":"0123456789abcdef0123456789abcdef","through_digest":"00","through_ordinal":"0"}}"#,
    r#"{"type":"compaction_message","provider":"mc","kind":"setup","compaction_id":"c","version":1,"range":{"from":0,"to":0},"replacement":[],"sha256":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","ceiling":"{\"at_share\":0.8,\"wait_ms\":1000}"}"#,
];

/// A rewrite ride's reported wait and its end (llm-runner 0.5.2 `compaction_wait`), each in a
/// gated frame requiring `compaction-ride-wait/v1`.
const RIDE_WAIT_PINNED: &[&str] = &[
    r#"{"type":"compaction_ride_wait","step_id":2,"deadline_ms":1790000004000}"#,
    r#"{"type":"compaction_ride_wait_ended","step_id":2}"#,
];

/// The ride-wait records keep their bytes and need `compaction-ride-wait/v1`; a Setup record carrying
/// the provider's `ride_wait_ms` requires `compaction-ride-wait/v1` (beside the ceiling's
/// `compaction-view/v1` when it carries one too), and one without it keeps its old bytes and
/// requirements.
#[test]
fn ride_wait_records_and_a_frozen_ride_wait_are_gated() {
    for pinned in RIDE_WAIT_PINNED {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), *pinned);
        assert_eq!(
            record.required_features(),
            &[crate::framing::COMPACTION_RIDE_WAIT_FEATURE],
            "{pinned}"
        );
    }
    let without = r#"{"type":"compaction_message","provider":"mc","kind":"setup","compaction_id":"c","version":1,"range":{"from":0,"to":0},"replacement":[],"sha256":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"}"#;
    let record: WalRecord = serde_json::from_str(without).unwrap();
    assert_eq!(serde_json::to_string(&record).unwrap(), without);
    assert!(!record
        .required_features()
        .contains(&crate::framing::COMPACTION_RIDE_WAIT_FEATURE));
    let with = r#"{"type":"compaction_message","provider":"mc","kind":"setup","compaction_id":"c","version":1,"range":{"from":0,"to":0},"replacement":[],"sha256":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","ride_wait_ms":4000}"#;
    let record: WalRecord = serde_json::from_str(with).unwrap();
    assert_eq!(serde_json::to_string(&record).unwrap(), with);
    assert!(record
        .required_features()
        .contains(&crate::framing::COMPACTION_RIDE_WAIT_FEATURE));
}

#[test]
fn asynchronous_compaction_records_keep_their_bytes_and_require_the_view_feature() {
    use crate::framing::{encode, GATED_FRAMING_VERSION};
    for pinned in COMPACTION_VIEW_PINNED {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), *pinned);
        let required = record.required_features();
        assert!(
            required.contains(&crate::framing::COMPACTION_VIEW_FEATURE),
            "{pinned}: {required:?}"
        );
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], GATED_FRAMING_VERSION, "{pinned}");
    }
}

/// The pinned list covers every kind this build can write, so no kind's bytes can drift
/// without a test failing. `Unknown`, the placeholder for a kind this build does not know, is
/// never written.
#[test]
fn the_pinned_bytes_cover_every_record_kind() {
    let billing = super::bill_attempt_tests::golden_records();
    let pinned: std::collections::BTreeSet<&str> = PINNED
        .iter()
        .copied()
        .chain(include_str!("../../tests/fixtures/compaction_records.jsonl").lines())
        .chain(DEFERRED_CALL_PINNED.iter().copied())
        .chain(CANCEL_AT_PINNED.iter().copied())
        .chain(PROVIDER_HELD_PINNED.iter().copied())
        .chain(DELIVERY_ORDER_PINNED.iter().copied())
        .chain(INPUT_CAP_PINNED.iter().copied())
        .chain(STRUCTURED_OUTPUT_PINNED.iter().copied())
        .chain(COMPACTION_VIEW_PINNED.iter().copied())
        .chain(RIDE_WAIT_PINNED.iter().copied())
        .chain(FALLBACK_PINNED.iter().map(|(bytes, _)| *bytes))
        .chain(OUTPUT_RESERVATION_PINNED.iter().copied())
        .chain(WRITER_ID_PINNED.iter().copied())
        .chain(billing.iter().map(String::as_str))
        .map(|bytes| tag_of(&serde_json::from_str::<WalRecord>(bytes).unwrap()))
        .collect();
    // The tag of every arm of the exhaustive `tag_of` except `WalRecord::Unknown`, the
    // placeholder for a kind this build does not know, which is never written. A new variant stops `tag_of`
    // compiling; whoever adds its arm must add its tag here too, and this assertion then fails
    // until `PINNED` holds bytes for it.
    let every_kind = [
        "writer_opened",
        "writer_closed",
        "run_started",
        "model_step_finished",
        "model_step_refused",
        "model_attempt_finished",
        "attempt_reserved",
        "attempt_finished",
        "attempt_released",
        "attempt_corrected",
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
        "structured_output_result",
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
        "compaction_view_applied",
        "compaction_notice",
        "tool_call_deferred",
        "approval_recorded",
        "awaiting_approval",
        "late_approval_recorded",
        "late_dispatch_intent",
        "late_result",
        "question_withdrawal_acknowledged",
        "tool_call_provider_held",
        "delivery_ordered",
        "send_ended",
        "send_delivered",
        "compaction_ride_wait",
        "compaction_ride_wait_ended",
        "output_reservation_clamped",
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
            cache_evidence: None,
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

/// Records that structured output extends, as this build writes them when a run has no
/// structured output: a run start whose frozen render config is populated, queued requests, a
/// hard-stop tool result, a re-frozen render config and a completed run end. Each is pinned with
/// its exact bytes and the exact frame requirements it carries. Structured output adds members
/// and a must-understand feature only to records that use it, so these must keep both unchanged:
/// a WAL with no structured output stays readable by builds that predate it.
const PRE_STRUCTURED_OUTPUT_PINNED: &[(&str, &[&str])] = &[
    (
        r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"edit","description":"edit a file","input_schema":{"type":"object"},"module":"aft","execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"family_id":"openai_responses","responses_tools_non_strict":true}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
        &[],
    ),
    (PINNED[1], &[]),
    (PINNED[39], &[]),
    (PINNED[40], &[]),
    (PINNED[41], &[]),
    (
        r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t2","output":{"kind":"error","message":"tool not run: max steps reached before dispatch"},"is_error":true},"outcome":"max_steps"}"#,
        &[],
    ),
    (
        r#"{"type":"run_config_refrozen","renderer_version":2,"frozen_render_config":{"family_id":"anthropic_messages"}}"#,
        &[],
    ),
    (
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
        &[],
    ),
];

#[test]
fn records_without_structured_output_keep_their_bytes_and_requirements() {
    for (pinned, requires) in PRE_STRUCTURED_OUTPUT_PINNED {
        let decoded: WalRecord = serde_json::from_str(pinned)
            .unwrap_or_else(|error| panic!("pinned record must decode: {error}\n{pinned}"));
        assert_ne!(decoded, WalRecord::Unknown, "{pinned}");
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
            *pinned,
            "a pinned record must re-encode to exactly the bytes it was read from"
        );
        assert_eq!(decoded.required_features(), *requires, "{pinned}");
    }
}

/// Records that carry structured-output state: a run start whose frozen render config holds the
/// run's frozen schema, a queued send asking for one, a completed run's result and a call
/// skipped because `submit_result` ended its run. Each requires `structured-output/v1`, so a
/// build that predates structured output refuses the session by name instead of resuming it
/// without its schema.
const STRUCTURED_OUTPUT_PINNED: &[&str] = &[
    r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"family_id":"openai_chat","structured_output":{"delivery":"strict","enforcement":"enforced","mode":"submit_tool","name":"result","profile":"xai","schema_jcs":"{\"type\":\"object\"}"}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
    r#"{"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[],"structured_output":{"mode":"final_text","schema":{"type":"object"}}}}"#,
    r#"{"type":"structured_output_result","run_id":"run-7","result":{"mode":"submit_tool","enforcement":"enforced","validation":"passed","value":{"answer":"x"},"skipped_siblings":[{"tool_call_id":"t2","tool":"read"}]}}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t2","output":{"kind":"error","message":"skipped_after_submit: tool not executed; submit_result ended the run in the same step"},"is_error":true},"outcome":"max_steps","reason":"skipped_after_submit"}"#,
];

#[test]
fn structured_output_records_are_must_understand_and_round_trip() {
    use crate::framing::{decode_next, encode, DecodeStep, STRUCTURED_OUTPUT_FEATURE};
    for bytes in STRUCTURED_OUTPUT_PINNED {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), *bytes);
        assert_eq!(
            record.required_features(),
            &[STRUCTURED_OUTPUT_FEATURE],
            "{bytes}"
        );
        let DecodeStep::Record { framed, .. } = decode_next(&encode(7, 3, &record).unwrap()) else {
            panic!("structured-output record must decode")
        };
        assert_eq!(framed.record, record);
    }
    // A run start that is also scoped requires both features, in sorted order.
    let scoped = format!(
        "{},\"scope\":{{\"owner\":{{\"kind\":\"reserved\",\"module_id\":\"m\"}},\"ref\":\"h\",\"scope_epoch\":1}}}}",
        &STRUCTURED_OUTPUT_PINNED[0][..STRUCTURED_OUTPUT_PINNED[0].len() - 1]
    );
    let scoped: WalRecord = serde_json::from_str(&scoped).unwrap();
    assert_eq!(
        scoped.required_features(),
        &[crate::framing::SCOPE_FEATURE, STRUCTURED_OUTPUT_FEATURE]
    );
    if let Some(path) = std::env::var_os("BROCA_STRUCTURED_OUTPUT_WAL_FIXTURE") {
        let structured: WalRecord = serde_json::from_str(STRUCTURED_OUTPUT_PINNED[0]).unwrap();
        let result: WalRecord = serde_json::from_str(STRUCTURED_OUTPUT_PINNED[2]).unwrap();
        let mut bytes = encode(1, 3, &structured).unwrap();
        bytes.extend(encode(2, 3, &result).unwrap());
        std::fs::write(&path, bytes).unwrap();
        let plain: WalRecord = serde_json::from_str(PINNED[1]).unwrap();
        let mut plain_path = path;
        plain_path.push(".plain");
        std::fs::write(plain_path, encode(1, 3, &plain).unwrap()).unwrap();
    }
}

/// A send's client_message_id lives in its identity digest, and a `RunStarted`, `Queued` or
/// `Steered` record whose digest carries it needs `client-message-id/v1`: a build without the
/// name would answer a re-send with another id as a plain retry. A digest without the member, a
/// record without a digest, and a digest that is not JSON leave the record ungated.
#[test]
fn a_client_message_id_in_the_identity_digest_requires_its_reader_feature() {
    use crate::framing::{decode_next, encode, DecodeStep, GATED_FRAMING_VERSION};
    const FEATURE: &str = "client-message-id/v1";
    let with_id = r#"{"version":1,"model":"m","input":"i","tools":"t","policy_allowed":"p","tool_choice":"c","generation":"g","cache":"k","budget":"b","stop_when":"s","client_message_id":"msg-1"}"#;
    let without_id = r#"{"version":1,"model":"m","input":"i","tools":"t","policy_allowed":"p","tool_choice":"c","generation":"g","cache":"k","budget":"b","stop_when":"s"}"#;
    let old = include_str!("../../tests/golden/pre_step_transforms/run_started.json").trim_end();
    let steered = serde_json::json!({"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"more"}]}]});
    let queued = serde_json::json!({"type":"queued", "submission_id":"sub", "request": {
        "model":{"provider_module_id":"p","model_id":"m"}, "input":[]}});
    for base in [serde_json::from_str(old).unwrap(), steered, queued] {
        let plain: WalRecord = serde_json::from_value(base.clone()).unwrap();
        assert!(!plain.required_features().contains(&FEATURE), "{base}");
        for (digest, gated) in [(with_id, true), (without_id, false), ("not json", false)] {
            let mut raw = base.clone();
            raw["request_identity_digest"] = serde_json::json!(digest);
            let record: WalRecord = serde_json::from_value(raw).unwrap();
            assert_eq!(
                record.required_features().contains(&FEATURE),
                gated,
                "{digest} on {base}"
            );
            if gated {
                let frame = encode(1, 1, &record).unwrap();
                assert_eq!(frame[4], GATED_FRAMING_VERSION);
                assert!(matches!(decode_next(&frame), DecodeStep::Record { .. }));
            }
        }
    }
}

#[test]
fn interrupt_reason_and_directives_require_named_reader_features_only_when_present() {
    use crate::framing::{decode_next, encode, DecodeStep, GATED_FRAMING_VERSION};
    for (field, value) in [
        ("interrupt_reason", serde_json::json!("runner_shutdown")),
        ("interrupt_reason", serde_json::json!("runner_crash")),
    ] {
        let mut raw = serde_json::json!({"type":"run_finished","reason":"interrupted","usage":{},"retries_used":0});
        raw[field] = value;
        let record: WalRecord = serde_json::from_value(raw).unwrap();
        assert_eq!(record.required_features(), &["interrupt-reason/v1"]);
        let frame = encode(1, 1, &record).unwrap();
        assert_eq!(frame[4], GATED_FRAMING_VERSION);
        assert!(matches!(decode_next(&frame), DecodeStep::Record { .. }));
    }
    let old = include_str!("../../tests/golden/pre_step_transforms/run_started.json").trim_end();
    let mut raw: serde_json::Value = serde_json::from_str(old).unwrap();
    let record: WalRecord = serde_json::from_value(raw.clone()).unwrap();
    let prior = record.required_features();
    raw["config"]["turn_directives_v1"] = serde_json::json!(["wake"]);
    let record: WalRecord = serde_json::from_value(raw).unwrap();
    assert!(record.required_features().contains(&"turn-directives/v1"));
    assert!(!prior.contains(&"turn-directives/v1"));
    let queued: WalRecord = serde_json::from_value(serde_json::json!({
        "type":"queued", "submission_id":"sub", "request": {
            "model":{"provider_module_id":"p","model_id":"m"}, "input":[], "directives":["wake"]
        }
    }))
    .unwrap();
    assert!(queued.required_features().contains(&"turn-directives/v1"));
    let old: WalRecord = serde_json::from_str(old).unwrap();
    assert_eq!(
        serde_json::to_string(&old).unwrap(),
        include_str!("../../tests/golden/pre_step_transforms/run_started.json").trim_end()
    );
}

/// Records holding tools a caller declared directly on `session.send` (no fetch plan), exactly as
/// builds wrote them before a declared tool could carry its own reply deadline. They must keep
/// these bytes and stay ungated: a session whose tools declare no deadline must remain readable
/// by every build that read it before.
const PRE_TOOL_REPLY_RUN_STARTED: &str = r#"{"type":"run_started","run_id":"run-9","session":{"project_root":"/tmp/project","harness":"runner","session":"lead"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"research","description":"Ask sidekick a question.","input_schema":{"properties":{"request":{"type":"string"}},"required":["request"],"type":"object"},"module":"prefrontal-core","execution_mode":"parallel"},{"name":"note","description":"Record a note.","input_schema":{"type":"object"},"execution_mode":"sequential"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;

#[test]
fn pre_tool_reply_send_declared_tools_keep_their_bytes_and_frame_requirements() {
    use crate::framing::{encode, FRAMING_VERSION, HEADER_LEN};
    for pinned in [
        PRE_TOOL_REPLY_RUN_STARTED,
        r#"{"type":"queued","submission_id":"sub-run-10","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"tools":[{"name":"research","description":"Ask sidekick a question.","input_schema":{"type":"object"},"module":"prefrontal-core","execution_mode":"parallel"}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#,
    ] {
        let record: WalRecord = serde_json::from_str(pinned).unwrap();
        assert!(
            matches!(
                record,
                WalRecord::RunStarted { .. } | WalRecord::Queued { .. }
            ),
            "{pinned}"
        );
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
        assert!(record.required_features().is_empty(), "{pinned}");
        let frame = encode(7, 3, &record).unwrap();
        assert_eq!(frame[4], FRAMING_VERSION);
        assert_eq!(&frame[HEADER_LEN..], pinned.as_bytes());
    }
}

/// The same run start once its `research` tool declares `reply: {max_ms: 1800000}`: the
/// deadline is the tool's last member, nothing else changes, and the frame requires
/// `tool-reply/v1`. The golden's tool entry is also checked against the bytes the real serve
/// path freezes for the `send_params.tool_reply.json` send.
#[test]
fn tool_reply_run_started_matches_golden() {
    let golden = include_str!("../../tests/golden/run_started.tool_reply.json");
    let mut record: WalRecord = serde_json::from_str(PRE_TOOL_REPLY_RUN_STARTED).unwrap();
    let WalRecord::RunStarted { config, .. } = &mut record else {
        panic!("expected a run start");
    };
    let mut tools = config.tools.as_ref().clone();
    tools[0].reply = Some(broca_protocol::ToolReply { max_ms: 1_800_000 });
    config.tools = std::sync::Arc::new(tools);
    assert_eq!(
        format!("{}\n", serde_json::to_string(&record).unwrap()),
        golden
    );
    assert_eq!(serde_json::from_str::<WalRecord>(golden).unwrap(), record);
    assert_eq!(
        record.required_features(),
        &[crate::framing::TOOL_REPLY_FEATURE]
    );
    assert_eq!(
        golden.trim_end(),
        PRE_TOOL_REPLY_RUN_STARTED.replacen(
            r#""execution_mode":"parallel"}"#,
            r#""execution_mode":"parallel","reply":{"max_ms":1800000}}"#,
            1
        )
    );
}
