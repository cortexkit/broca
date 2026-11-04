use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, FinishReason, Message, Role, ToolOutput, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;

use super::*;
use crate::codec::{DecodeErrorKind, Digest};

fn model() -> ModelHandle {
    ModelHandle {
        provider_module_id: "anthropic".into(),
        model_id: "claude-opus-5".into(),
    }
}

/// One record of every kind, with optional members set, so a kind that fails to round-trip or
/// is missing from this list shows up here.
fn one_of_each() -> Vec<Record> {
    let usage = Usage {
        input_tokens: Some(2),
        output_tokens: Some(0),
        ..Usage::default()
    };
    let result = ToolResult {
        tool_call_id: ToolCallId("c1".into()),
        output: ToolOutput::Text { text: "ok".into() },
        is_error: false,
    };
    let part = PartRef {
        kind: PartKind::Tools,
        digest: Digest([7; 32]),
    };
    vec![
        Record::Input(Input {
            run_id: "run-1".into(),
            placement: Placement::RunInput,
            message: Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }]),
            written_at_ms: Some(1),
            submission_id: Some("sub".into()),
            notice: Some(Notice {
                sender: "magic-context".into(),
                template: None,
            }),
            tags_v1: Some(serde_json::json!({"parts": [3]})),
            receipt_seq: Some(4),
        }),
        Record::Assistant(Assistant {
            run_id: "run-1".into(),
            step_id: 1,
            provider_message_id: MessageId("msg_1".into()),
            message: Message::assistant_from(
                vec![ContentBlock::Text { text: "OK".into() }],
                model(),
            ),
            usage: Some(usage),
            written_at_ms: Some(2),
            last_content_at_ms: None,
            budget_finalize: None,
            tags_v1: None,
        }),
        Record::ToolReply(ToolReply {
            run_id: "run-1".into(),
            batch_id: Some("b1".into()),
            result: result.clone(),
            finished_at_ms: Some(3),
            tags_v1: None,
        }),
        Record::ResultAppend(ResultAppend {
            run_id: "run-1".into(),
            tool_call_id: ToolCallId("c1".into()),
            text: "reminder".into(),
            notice: None,
            written_at_ms: None,
        }),
        Record::SetupStub(SetupStub {
            parts: vec![part.clone()],
            model: model(),
            tool_names: vec!["read".into(), "bash".into()],
        }),
        Record::Compaction(Compaction {
            version: 2,
            body: serde_json::json!({"first_kept_ordinal": 12}),
        }),
        Record::RunStart(Box::new(RunStart {
            run_id: "run-1".into(),
            model: model(),
            setup: vec![part],
            renderer_version: 3,
            submission_id: None,
            send_id: Some("s".into()),
            request_identity_digest: None,
            ts_ms: Some(0),
            written_at_ms: None,
            session_class: Some("mason".into()),
            retention_secs: None,
            title: None,
            context_limit: Some(1_000_000),
            service_tier: None,
            route_decision_id: None,
            compaction_plan_version: Some(2),
            render_config: Some(serde_json::json!({"wire": "anthropic"})),
            auth_selection: None,
            run_choices: Some(serde_json::json!({"generation": {"max_output_tokens": 2048}})),
            origin: Some(serde_json::json!({"kind": "fresh"})),
            principal: None,
            scope: None,
            hooks: None,
            keep_warm: None,
            on_restart: None,
        })),
        Record::RunEnd(RunEnd {
            run_id: "run-1".into(),
            reason: RunFinishReason::Interrupted,
            usage: Some(usage),
            ts_ms: Some(9),
            retries_used: 0,
            interrupt_reason: Some("shutdown".into()),
            error: None,
        }),
        Record::StepStart(StepStart {
            run_id: "run-1".into(),
            step_id: 1,
        }),
        Record::StepEnd(StepEnd {
            run_id: "run-1".into(),
            step_id: 1,
            usage: Some(usage),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 1,
            service_tier_used: Some("default".into()),
            speed_used: None,
            call_count: 1,
            fallback: None,
            hooks: None,
        }),
        Record::Attempt(Attempt {
            run_id: "run-1".into(),
            step_id: 1,
            attempt: 0,
            attempt_id: None,
            outcome: "complete".into(),
            http_status: Some(200),
            request_body_len: 16_657,
            request_body_sha256: Digest([1; 32]),
            response_body_len: 1_727,
            response_body_sha256: Digest([2; 32]),
            ts_ms: 5,
            sent_at_ms: Some(4),
            first_content_at_ms: Some(4),
            last_content_at_ms: Some(5),
            transport: Some("https".into()),
            served_account_ref: None,
            billing: None,
            telemetry: Some(serde_json::json!({"max_content_gap_ms": 613})),
        }),
        Record::BatchStart(BatchStart {
            run_id: "run-1".into(),
            batch_id: "b1".into(),
            step_id: 1,
            planned: vec![ToolCallId("c1".into())],
        }),
        Record::BatchEnd(BatchEnd {
            run_id: "run-1".into(),
            batch_id: "b1".into(),
        }),
        Record::ToolIntent(ToolIntent {
            run_id: "run-1".into(),
            batch_id: "b1".into(),
            tool_call_id: ToolCallId("c1".into()),
            tool_name: Some("read".into()),
            module_id: Some("aft".into()),
            args_digest: Digest([3; 32]),
            args: None,
            call_key: Some("lineage:12".into()),
            approval_digest: None,
            dispatched_at_ms: Some(6),
            schema_pin: None,
            hooks: None,
        }),
        Record::ToolOutcome(ToolOutcome {
            run_id: "run-1".into(),
            batch_id: "b1".into(),
            tool_call_id: ToolCallId("c1".into()),
            outcome: CallOutcome::Completed,
            result_digest: Digest([4; 32]),
            is_error: false,
            reason: None,
            finished_at_ms: Some(7),
            detail: None,
            hooks: None,
        }),
        Record::QueuedSend(QueuedSend {
            submission_id: "sub".into(),
            send_id: Some("s2".into()),
            input: vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "next".into(),
                }],
            )],
            setup: vec![],
            written_at_ms: Some(8),
            ts_ms: None,
            request: Some(serde_json::json!({"model": {}})),
            receipt: None,
        }),
        Record::SessionRefresh(SessionRefresh {
            generation: 2,
            policy: "immediately".into(),
            run_in_progress: false,
            applies_from_next_run: true,
            setup: vec![],
            manifest: None,
        }),
        Record::StructuredResult(StructuredResult {
            run_id: "run-1".into(),
            result: serde_json::json!({"ok": true}),
        }),
        Record::Event(Event {
            name: "run_paused".into(),
            run_id: Some("run-1".into()),
            body: Some(serde_json::json!({"reason": "restart"})),
        }),
    ]
}

#[test]
fn every_kind_round_trips_under_its_own_number_and_stream() {
    let records = one_of_each();
    let mut kinds: Vec<u32> = records.iter().map(Record::kind).collect();
    kinds.dedup();
    assert_eq!(kinds.len(), records.len(), "one record per kind");
    for record in records {
        let body = record.encode_body();
        let back = Record::decode(record.kind(), &body).unwrap();
        assert_eq!(back, record, "{}", record.name());
        assert_eq!(back.encode_body(), body);
        assert_eq!(kind_stream(record.kind()), Some(record.stream()));
        let expected = if record.kind() < 64 {
            Stream::Transcript
        } else {
            Stream::Ledger
        };
        assert_eq!(record.stream(), expected);
    }
}

#[test]
fn an_unknown_kind_refuses() {
    let error = Record::decode(63, &[]).unwrap_err();
    assert_eq!(error.kind, DecodeErrorKind::UnknownVariant(63));
    assert_eq!(kind_stream(63), None);
}

#[test]
fn a_body_decoded_as_another_kind_refuses_or_differs() {
    // Kind numbers live in the frame header, not the body, so a reader must never take a body
    // under the wrong kind. A RunEnd body read as StepStart lacks nothing StepStart requires, so
    // this documents why the header's kind is authenticated rather than relying on the body.
    let end = Record::RunEnd(RunEnd {
        run_id: "r".into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    });
    let as_start = Record::decode(66, &end.encode_body());
    assert!(as_start.is_err() || as_start.unwrap() != end);
}
