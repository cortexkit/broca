//! The inverse on records only the new storage writes, and on a log taken a batch at a time.
//! Converted logs never hold the first kind, so the round trips in `broca-session-convert`
//! don't reach them; a live session's log always does.

use std::collections::BTreeMap;

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_session_store::codec::Id128;
use broca_session_store::record::{
    ChangeAborted, ChangeIntent, ChangeKind, Event, Input, Placement, Record, RecoveryCut, Steer,
    TranscriptReverted, WriterOpened,
};

use super::{invert, Inverse, InverseError};

fn no_parts(_: &broca_session_store::record::PartRef) -> Result<serde_json::Value, String> {
    Err("no setup parts in this log".into())
}

fn event(name: &str) -> Record {
    Record::Event(Event {
        name: name.into(),
        run_id: None,
        body: None,
    })
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn steer(text: &str) -> Record {
    Record::Steer(Steer {
        named_speaker: None,
        input: vec![user(text)],
        written_at_ms: None,
        tags_v1: None,
        receipt: None,
        media: Vec::new(),
        message_marks: None,
    })
}

fn placed_copy(text: &str, receipt_seq: u64) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: "r".into(),
        placement: Placement::Steer,
        message: std::sync::Arc::new(user(text)),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: Some(receipt_seq),
        pruned: Vec::new(),
    })
}

#[test]
fn storage_bookkeeping_records_have_no_today_record_and_are_skipped() {
    let change = Id128([7; 16]);
    let records = [
        Record::WriterOpened(WriterOpened { generation: change }),
        event("first"),
        Record::ChangeIntent(ChangeIntent {
            change,
            kind: ChangeKind::LedgerSeal,
            cut: None,
        }),
        Record::ChangeAborted(ChangeAborted { change }),
        event("second"),
    ];
    let names: Vec<_> = invert(&records, &BTreeMap::new())
        .unwrap()
        .iter()
        .map(|value| value["type"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["first", "second"]);
}

#[test]
fn a_revert_or_a_recovery_cut_is_refused_by_name_never_a_panic() {
    let change = Id128([9; 16]);
    let reverted = Record::TranscriptReverted(TranscriptReverted {
        change,
        from_seq: 2,
        through_seq: 3,
        ordinal: 1,
        history_version: 1,
        tag_high: None,
    });
    let cut = Record::RecoveryCut(RecoveryCut {
        change,
        at: 4,
        durable_bound: 3,
        removed_from: 4,
        removed_through: 5,
        loss_aware: true,
        transcript_from: None,
        ordinal: None,
        history_version: None,
        unattributed_risk: false,
        saved: String::new(),
    });
    for (record, name) in [(reverted, "TranscriptReverted"), (cut, "RecoveryCut")] {
        let error = Inverse::new()
            .push(1, &[event("before"), record], &no_parts)
            .unwrap_err();
        assert_eq!(error, InverseError::NoTodayRecord(name));
    }
}

#[test]
fn batches_must_follow_each_other_without_a_gap() {
    let mut inverse = Inverse::new();
    inverse.push(1, &[event("one")], &no_parts).unwrap();
    // Seq 2 was never taken in: the next batch can't start at seq 3.
    let error = inverse.push(3, &[event("three")], &no_parts).unwrap_err();
    assert!(matches!(error, InverseError::Layout(_)), "{error:?}");
    assert_eq!(inverse.next_seq(), 2);
}

#[test]
fn a_steer_placed_in_a_later_batch_is_checked_against_its_copies() {
    // The steer is accepted at seq 1; its message is placed at the next step boundary.
    let mut inverse = Inverse::new();
    let steered = inverse.push(1, &[steer("look")], &no_parts).unwrap();
    assert_eq!(steered[0]["type"], "steered");
    assert!(inverse
        .push(2, &[placed_copy("look", 1), event("step")], &no_parts)
        .is_ok());

    // A copy that differs from the message the steer accepted is refused.
    let mut inverse = Inverse::new();
    inverse.push(1, &[steer("look")], &no_parts).unwrap();
    let error = inverse
        .push(2, &[placed_copy("changed", 1)], &no_parts)
        .unwrap_err();
    assert!(error.to_string().contains("steered"), "{error}");

    // A copy naming a record that accepted nothing is refused.
    let mut inverse = Inverse::new();
    inverse.push(1, &[event("not a steer")], &no_parts).unwrap();
    assert!(inverse
        .push(2, &[placed_copy("look", 1)], &no_parts)
        .is_err());
}

/// The start of an import's seeded run, as `session.import` writes it: marked seeded, with an
/// import origin and today's import run config.
fn import_start(run_id: &str) -> broca_session_store::record::RunStart {
    let config = broca_core::import::import_marker_config("digest", 40);
    broca_session_store::record::RunStart {
        run_id: run_id.into(),
        model: broca_protocol::ModelHandle::new("import", "import"),
        setup: Vec::new(),
        renderer_version: 0,
        submission_id: None,
        send_id: Some("imp-1".into()),
        request_identity_digest: Some("digest".into()),
        ts_ms: Some(9_000),
        written_at_ms: Some(9_000),
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: Some(broca_core::compaction_v2::PLAN_VERSION),
        auth_selection: None,
        run_choices: Some(serde_json::json!({ "config": config })),
        origin: Some(
            serde_json::to_value(broca_wal::RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                source_ref: "thalamus:ses_head".into(),
                origin_device_pseudonym: None,
                base_seq: None,
            })
            .unwrap(),
        ),
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: Some(true),
        refusal_schedule: None,
        imported_ids: None,
        message_marks: None,
        named_speaker: None,
    }
}

/// A later run's start: no seed, no origin, the same config shape.
fn plain_start(run_id: &str) -> broca_session_store::record::RunStart {
    broca_session_store::record::RunStart {
        seeded: None,
        origin: None,
        send_id: None,
        request_identity_digest: None,
        ..import_start(run_id)
    }
}

fn input_record(run_id: &str, placement: Placement, message: Message) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: run_id.into(),
        placement,
        message: std::sync::Arc::new(message),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

/// The facts `session.import` writes after an imported message.
fn entry_facts(ordinal: u64, mid: &str) -> Record {
    Record::EntryFacts(std::sync::Arc::new(
        broca_session_store::record::EntryFacts {
            send_id: None,
            ordinal,
            written_at_ms: Some(1_000),
            tag_base: 40,
            notice: None,
            usage: None,
            generated_under: None,
            native: false,
            render_tags: false,
            clean_reply: false,
            mid: mid.into(),
            run_id: None,
            created_at_ms: None,
            delivery_mode: None,
            delivery_tool_call_id: None,
            client_message_id: None,
            sender: None,
            marks: None,
            named_speaker: broca_session_store::record::marks_name_speaker(None),
            no_display_facts: None,
            reasoning_summaries: Vec::new(),
            rich_ui: None,
            notices: Vec::new(),
            runner_tag_base: None,
            legacy_view_seq: None,
        },
    ))
}

fn run_end(run_id: &str) -> Record {
    Record::RunEnd(broca_session_store::record::RunEnd {
        run_id: run_id.into(),
        reason: broca_protocol::RunFinishReason::Completed,
        usage: None,
        ts_ms: Some(9_000),
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

/// An imported history: a prompt, a reply that calls a tool, the call's result with a note
/// appended to it (a `ResultAppend`), and a last reply.
fn imported_messages() -> Vec<Message> {
    let call = broca_protocol::content::ToolCall {
        tool_call_id: broca_protocol::ToolCallId("call_1".into()),
        tool_name: "read".into(),
        input: serde_json::json!({"path": "a"}).into(),
        thought_signature: None,
    };
    vec![
        user("Look."),
        Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Text {
                    text: "Reading.".into(),
                },
                ContentBlock::ToolCall(call),
            ],
        ),
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(
                broca_protocol::content::ToolResult {
                    tool_call_id: broca_protocol::ToolCallId("call_1".into()),
                    output: broca_protocol::content::ToolOutput::Text {
                        text: "contents".into(),
                    },
                    is_error: false,
                },
            )],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::Text {
                text: "Done.".into(),
            }],
        ),
    ]
}

/// An import's seeded run as `session.import` lays it out: its start, each message with its
/// facts after it (a turn's call facts and the notes appended to its results between them),
/// its end.
fn import_batch(run_id: &str) -> Vec<Record> {
    let messages = imported_messages();
    let mut records = vec![Record::RunStart(Box::new(import_start(run_id)))];
    for (index, message) in messages.into_iter().enumerate() {
        let ordinal = index as u64 + 1;
        let calls = message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolCall(_)));
        let results = message.role == Role::Tool;
        records.push(input_record(run_id, Placement::Imported, message));
        if calls {
            records.push(Record::TurnFacts(std::sync::Arc::new(
                broca_session_store::record::TurnFacts {
                    ordinal,
                    calls: vec![broca_session_store::record::CallFacts {
                        tool_call_id: broca_protocol::ToolCallId("call_1".into()),
                        call_key: "msg_a1/call_1".into(),
                        schema_pin: None,
                        carrier: Some("thalamus-harness".into()),
                        dispatched_to: None,
                        dispatched_at_ms: None,
                        finished_at_ms: None,
                        unanswered: None,
                    }],
                },
            )));
        }
        if results {
            records.push(Record::ResultAppend(
                broca_session_store::record::ResultAppend {
                    run_id: run_id.into(),
                    tool_call_id: broca_protocol::ToolCallId("call_1".into()),
                    text: "\n\n[occi]".into(),
                    notice: Some(broca_session_store::record::Notice {
                        sender: "occi".into(),
                        template: None,
                    }),
                    written_at_ms: None,
                    send_id: Some("s2".into()),
                    ride: None,
                },
            ));
        }
        records.push(entry_facts(ordinal, &format!("m{ordinal}")));
    }
    records.push(run_end(run_id));
    records
}

/// An import's seeded run inverts to what today's import seed is: one `run_started` holding
/// every imported message in order, with the import's origin, then the run's end. Its facts
/// records and appended notes are taken in, and nothing is left waiting for a record to claim
/// it. A run after it, in a later batch, inverts as any run does.
#[test]
fn an_imports_seeded_run_inverts_to_one_run_started_holding_every_message() {
    let batch = import_batch("seed");
    let mut inverse = Inverse::new();
    let out = inverse.push(1, &batch, &no_parts).unwrap();
    let names: Vec<_> = out
        .iter()
        .map(|value| value["type"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["run_started", "run_finished"]);
    inverse.finish().unwrap();
    assert_eq!(inverse.next_seq(), batch.len() as u64 + 1);

    let input: Vec<Message> = serde_json::from_value(out[0]["input"].clone()).unwrap();
    assert_eq!(input, imported_messages(), "every message, in order");
    // Today's log can hold it, and reads it as an import's seed, as today's placement does.
    let mut started = out[0].clone();
    started["session"] =
        serde_json::to_value(broca_protocol::BindIdentity::new("/p", "h", "s")).unwrap();
    let record: broca_wal::WalRecord = serde_json::from_value(started).unwrap();
    let broca_wal::WalRecord::RunStarted { origin, input, .. } = record else {
        panic!("not a run start")
    };
    assert_eq!(input.len(), 4);
    assert!(
        matches!(
            origin,
            broca_wal::RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                ..
            }
        ),
        "{origin:?}"
    );
    // Its end, as today's import seed ends: with zero usage, which today's record requires.
    let finished: broca_wal::WalRecord = serde_json::from_value(out[1].clone()).unwrap();
    let broca_wal::WalRecord::RunFinished { usage, .. } = finished else {
        panic!("not a run end")
    };
    assert_eq!(usage, broca_protocol::Usage::default());

    // The owner's next run, in its own batch.
    let next = [
        Record::RunStart(Box::new(plain_start("r1"))),
        input_record("r1", Placement::RunInput, user("And now?")),
        run_end("r1"),
    ];
    let out = inverse
        .push(batch.len() as u64 + 1, &next, &no_parts)
        .unwrap();
    let expected = Inverse::new().push(1, &next, &no_parts).unwrap();
    assert_eq!(out, expected);
    assert_eq!(out[0]["input"][0]["content"][0]["text"], "And now?");
    assert!(out[0].get("origin").is_none());
    inverse.finish().unwrap();
}

/// Only an import's seeded run claims its imported messages so: a seeded run that doesn't end
/// in the batch that starts it, or that holds a record an import never writes, is refused by
/// name, and an imported message outside a seeded run is left unclaimed as before.
#[test]
fn an_imported_message_outside_its_whole_seeded_run_is_refused() {
    let batch = import_batch("seed");
    let cut = &batch[..batch.len() - 1];
    let error = Inverse::new().push(1, cut, &no_parts).unwrap_err();
    assert!(
        error.to_string().contains("doesn't end in the batch"),
        "{error}"
    );

    let mut stray = batch.clone();
    stray.insert(2, event("stray"));
    let error = Inverse::new().push(1, &stray, &no_parts).unwrap_err();
    assert!(error.to_string().contains("Event"), "{error}");

    let loose = [
        input_record("seed", Placement::Imported, user("Look.")),
        event("after"),
    ];
    assert!(Inverse::new().push(1, &loose, &no_parts).is_err());

    // A seeded start whose inputs are placed as ordinary run inputs (`Placement::RunInput`, as
    // the converter and an import of inline messages write them) inverts as any run does: its
    // inputs follow it directly.
    let converted = [
        Record::RunStart(Box::new(import_start("seed"))),
        input_record("seed", Placement::RunInput, user("Look.")),
        input_record("seed", Placement::RunInput, user("Again.")),
        run_end("seed"),
    ];
    let mut inverse = Inverse::new();
    let out = inverse.push(1, &converted, &no_parts).unwrap();
    inverse.finish().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["input"].as_array().unwrap().len(), 2);
    assert!(
        out[1].get("usage").is_none(),
        "its end is inverted as recorded"
    );
}
