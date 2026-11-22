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

#[test]
fn cache_write_lifetimes_roundtrip_in_step_end_record() {
    let mut record = one_of_each()
        .into_iter()
        .find(|record| matches!(record, Record::StepEnd(_)))
        .unwrap();
    let Record::StepEnd(end) = &mut record else {
        unreachable!()
    };
    end.usage = Some(Usage {
        cache_write_tokens: Some(30),
        cache_write_5m_tokens: Some(10),
        cache_write_1h_tokens: Some(20),
        ..Usage::default()
    });
    end.total_usage = end.usage;
    let bytes = record.encode_body();
    assert_eq!(Record::decode(record.kind(), &bytes).unwrap(), record);
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
            media: Vec::new(),
            run_id: "run-1".into(),
            placement: Placement::RunInput,
            message: std::sync::Arc::new(Message::new(
                Role::User,
                vec![ContentBlock::Text { text: "hi".into() }],
            )),
            written_at_ms: Some(1),
            submission_id: Some("sub".into()),
            notice: Some(Notice {
                sender: "magic-context".into(),
                template: None,
            }),
            tags_v1: Some(serde_json::json!({"parts": [3]})),
            receipt_seq: Some(4),
            pruned: Vec::new(),
        }),
        Record::Assistant(Assistant {
            run_id: "run-1".into(),
            step_id: 1,
            provider_message_id: MessageId("msg_1".into()),
            message: std::sync::Arc::new(Message::assistant_from(
                vec![ContentBlock::Text { text: "OK".into() }],
                model(),
            )),
            usage: Some(usage),
            written_at_ms: Some(2),
            last_content_at_ms: None,
            budget_finalize: None,
            tags_v1: None,
            pruned: Vec::new(),
        }),
        Record::ToolReply(ToolReply {
            run_id: "run-1".into(),
            batch_id: Some("b1".into()),
            result: result.clone(),
            finished_at_ms: Some(3),
            tags_v1: None,
            media: Vec::new(),
        }),
        Record::ResultAppend(ResultAppend {
            send_id: None,
            run_id: "run-1".into(),
            tool_call_id: ToolCallId("c1".into()),
            text: "reminder".into(),
            notice: None,
            written_at_ms: None,
            ride: Some(RideEnvelope {
                named_speaker: None,
                call_key: "lineage:4".into(),
                sender: "reminders".into(),
                forms: vec!["tool_result".into(), "turn_system".into()],
                form: RideForm::Append,
                speaks_for: None,
            }),
        }),
        Record::SetupStub(SetupStub {
            parts: vec![part.clone()],
            model: model(),
            tool_names: vec!["read".into(), "bash".into()],
            render_key: Some(Digest([4; 32])),
        }),
        Record::Compaction(Compaction {
            version: 2,
            body: serde_json::json!({"first_kept_ordinal": 12}).into(),
        }),
        Record::RunStart(Box::new(RunStart {
            named_speaker: None,
            kept_notice: None,
            imported_ids: None,
            message_marks: None,
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
            auth_selection: None,
            run_choices: Some(serde_json::json!({"generation": {"max_output_tokens": 2048}})),
            origin: Some(serde_json::json!({"kind": "fresh"})),
            principal: None,
            scope: None,
            hooks: None,
            keep_warm: None,
            on_restart: None,
            seeded: None,
            refusal_schedule: None,
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
            mid: Some("gabc.4".into()),
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
            budget_finalize: None,
            total_usage: None,
            stop_met: None,
            limiting_stop_met: None,
            by_model: vec![ModelStepUsage {
                provider: "anthropic".into(),
                model: "claude".into(),
                variant: Some("high".into()),
                usage,
            }],
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
            telemetry: Some(serde_json::json!({"max_content_gap_ms": 613}).into()),
            reached_cache: Some(true),
            requested_cache_ttl_ms: Some(300_000),
        }),
        Record::BatchStart(BatchStart {
            run_id: "run-1".into(),
            batch_id: "b1".into(),
            step_id: 1,
            planned: vec![ToolCallId("c1".into())],
            mid: Some("gabc.9".into()),
            shown_tags: Vec::new(),
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
            key_derived: None,
        }),
        Record::ToolOutcome(ToolOutcome {
            yielded_by: None,
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
            call_key: None,
        }),
        Record::QueuedSend(QueuedSend {
            named_speaker: None,
            kept_notice: None,
            media: Vec::new(),
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
            request: Some(serde_json::json!({"model": {}}).into()),
            receipt: None,
            delivery: Some("interrupt".into()),
            message_marks: None,
        }),
        Record::SessionRefresh(SessionRefresh {
            generation: 2,
            policy: "immediately".into(),
            run_in_progress: false,
            applies_from_next_run: true,
            setup: vec![],
            manifest: None,
            selection: Some(serde_json::json!({"provider": "p", "model": "m"})),
            refetch: Vec::new(),
        }),
        Record::RefreshPolicy(RefreshPolicy {
            generation: 2,
            policy: "on_prefix_rebuild".into(),
            send_id: "policy-1".into(),
        }),
        Record::PrefixFlush(PrefixFlush {
            send_id: "flush-1".into(),
            pending_generation: Some(2),
        }),
        Record::RefreshApplied(RefreshApplied {
            generation: 2,
            applied_by: "fold".into(),
            manifest_digest: "m".into(),
            composition_digest: Some("c".into()),
            absent_items: vec![serde_json::json!({"provider": "p", "kind": "tool"})],
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
fn source_lineage_round_trips_with_a_must_understand_bit() {
    let moved = Moved {
        move_id: "move".into(),
        source_activity_ms: 9,
        source_lineage: Some("0190a6b2-0000-7000-8000-000000000001".into()),
    };
    let record = Record::Moved(moved);
    let body = record.encode_body();
    assert_eq!(Record::decode(record.kind(), &body).unwrap(), record);
    let field = crate::codec::Fields::new(&body, "old reader")
        .map(Result::unwrap)
        .find(|field| field.number == 3)
        .unwrap();
    assert!(field.must);
    assert!(matches!(
        crate::codec::skip_unknown(field, "old reader")
            .unwrap_err()
            .kind,
        crate::codec::DecodeErrorKind::MustUnderstand { field: 3 }
    ));
    let old_move = Record::Moved(Moved {
        move_id: "move".into(),
        source_activity_ms: 9,
        source_lineage: None,
    });
    let body = old_move.encode_body();
    assert_eq!(Record::decode(old_move.kind(), &body).unwrap(), old_move);
    assert!(!crate::codec::Fields::new(&body, "old move")
        .map(Result::unwrap)
        .any(|field| field.number == 3));
}

#[test]
fn a_fork_fact_keeps_its_kind_number_and_bytes() {
    // A fork's facts are read back by later builds, so their kind number and encoding are
    // pinned: field numbers in order, the event as its declared number, usage as compact JSON.
    let record = Record::Fork(Fork {
        fork_id: "f".into(),
        event: ForkEvent::AskFinished,
        at_ms: 300,
        ask_id: Some("a".into()),
        attempt: Some(2),
        question: None,
        answer: Some("x".into()),
        finish: Some("end_turn".into()),
        usage: Some(serde_json::json!({"input_tokens": 1})),
        principal: None,
        base: None,
        admission_id: None,
        idle_ms: None,
        retain_ms: None,
        detail: None,
    });
    assert_eq!(record.kind(), 107);
    assert_eq!(record.stream(), Stream::Ledger);
    let body = record.encode_body();
    assert_eq!(
        crate::codec::hex(&body),
        concat!(
            "120166",
            "2004",
            "30ac02",
            "420161",
            "5002",
            "720178",
            "820108656e645f7475726e",
            "9201127b22696e7075745f746f6b656e73223a317d",
        )
    );
    assert_eq!(Record::decode(107, &body).unwrap(), record);
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

/// Part numbers fold into the fewest ranges, and a range set holds exactly the numbers it was
/// made from, gaps included.
#[test]
fn tag_ranges_hold_exactly_their_numbers() {
    let tags = [1, 2, 3, 5, 7, 8, 9, u64::MAX];
    let ranges = TagRange::of_sorted(&tags);
    assert_eq!(
        ranges,
        [
            TagRange { first: 1, last: 3 },
            TagRange { first: 5, last: 5 },
            TagRange { first: 7, last: 9 },
            TagRange {
                first: u64::MAX,
                last: u64::MAX
            },
        ]
    );
    for tag in 0..=12 {
        assert_eq!(TagRange::holds(&ranges, tag), tags.contains(&tag), "{tag}");
    }
    assert!(TagRange::holds(&ranges, u64::MAX));
    assert!(TagRange::of_sorted(&[]).is_empty());
    assert!(!TagRange::holds(&[], 1));
}
