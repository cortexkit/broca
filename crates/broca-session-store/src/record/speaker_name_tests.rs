//! Display marks as the new storage stores them before a speaker could carry a display name: a
//! speaker without a name, and a notice. A named speaker is gated by a must-understand field of
//! its own, so these records must keep exactly these bytes.

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::ToolCallId;
use broca_protocol::request::ModelHandle;
use serde_json::json;

use super::*;
use crate::codec::Digest;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn speaker() -> serde_json::Value {
    json!({"agent_id":"Ada"})
}

fn text(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn ride() -> RideEnvelope {
    RideEnvelope {
        named_speaker: None,
        call_key: "lineage:4".into(),
        sender: "reminders".into(),
        forms: vec!["tool_result".into()],
        form: RideForm::Append,
        speaks_for: Some(speaker()),
    }
}

fn entry_facts(marks: serde_json::Value) -> EntryFacts {
    EntryFacts {
        send_id: None,
        named_speaker: None,
        ordinal: 3,
        written_at_ms: Some(10),
        tag_base: 2,
        notice: None,
        usage: None,
        generated_under: None,
        native: false,
        render_tags: true,
        clean_reply: false,
        mid: "gabc.3".into(),
        run_id: Some("run-1".into()),
        created_at_ms: Some(9),
        delivery_mode: Some("steer".into()),
        delivery_tool_call_id: None,
        client_message_id: None,
        sender: Some("core".into()),
        marks: Some(marks.into()),
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: None,
        legacy_view_seq: None,
        notices: vec![RideNotice {
            named_speaker: None,
            part: 0,
            send_id: "s1".into(),
            sender: "reminders".into(),
            call_key: "lineage:4".into(),
            text: "reminder".into(),
            speaks_for: Some(speaker()),
        }],
    }
}

/// One record of each kind that stores marks, each holding an unnamed speaker (and the steer a
/// notice), as builds before speaker names wrote them.
fn marked() -> Vec<Record> {
    let model = ModelHandle {
        provider_module_id: "anthropic".into(),
        model_id: "claude-opus-5".into(),
    };
    let marks = json!({"speaks_for": speaker()});
    vec![
        Record::ResultAppend(ResultAppend {
            send_id: Some("s1".into()),
            run_id: "run-1".into(),
            tool_call_id: ToolCallId("c1".into()),
            text: "reminder".into(),
            notice: None,
            written_at_ms: Some(4),
            ride: Some(ride()),
        }),
        Record::EntryFacts(std::sync::Arc::new(entry_facts(marks.clone()))),
        Record::RunStart(Box::new(RunStart {
            named_speaker: None,
            imported_ids: None,
            message_marks: Some(marks.clone()),
            run_id: "run-1".into(),
            model,
            setup: vec![PartRef {
                kind: PartKind::Tools,
                digest: Digest([7; 32]),
            }],
            renderer_version: 3,
            submission_id: Some("sub".into()),
            send_id: Some("s".into()),
            request_identity_digest: None,
            ts_ms: Some(0),
            written_at_ms: None,
            session_class: None,
            retention_secs: None,
            title: None,
            context_limit: Some(1_000_000),
            service_tier: None,
            route_decision_id: None,
            compaction_plan_version: None,
            auth_selection: None,
            run_choices: None,
            origin: Some(json!({"kind": "fresh"})),
            principal: None,
            scope: None,
            hooks: None,
            keep_warm: None,
            on_restart: None,
            seeded: None,
            refusal_schedule: None,
        })),
        Record::QueuedSend(QueuedSend {
            named_speaker: None,
            media: Vec::new(),
            submission_id: "sub".into(),
            send_id: Some("s2".into()),
            input: vec![text("next")],
            setup: vec![],
            written_at_ms: Some(8),
            ts_ms: None,
            request: Some(json!({"model": {}})),
            receipt: None,
            delivery: None,
            message_marks: Some(marks),
        }),
        Record::Steer(Steer {
            named_speaker: None,
            input: vec![text("steer")],
            written_at_ms: Some(5),
            tags_v1: None,
            receipt: None,
            media: Vec::new(),
            message_marks: Some(json!({"notice": true})),
        }),
    ]
}

/// The bodies of [`marked`], encoded before speaker names existed.
const PINNED: [(&str, &str); 5] = [
    (
        "ResultAppend",
        "120572756e2d3122026331320872656d696e64657250046a0273317a3912096c696e656167653a34220972656d696e64657273320b746f6f6c5f726573756c7440015a127b226167656e745f6964223a22416461227d",
    ),
    (
        "EntryFacts",
        "1003200a3002800101a20106676162632e33b2010572756e2d31c00109d201057374656572820204636f72659202217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7df2023822027331320972656d696e6465727342096c696e656167653a34520872656d696e6465726a127b226167656e745f6964223a22416461227d",
    ),
    (
        "RunStart",
        "120572756e2d31221a1209616e7468726f706963220d636c617564652d6f7075732d35322410012220070707070707070707070707070707070707070707070707070707070707070740035203737562620173800100d001c0843dc202107b226b696e64223a226672657368227dda03217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7d",
    ),
    (
        "QueuedSend",
        "120373756222027332320c10022208220612046e6578745008720c7b226d6f64656c223a7b7d7dba01217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7d",
    ),
    (
        "Steer",
        "120d1002220922071205737465657220056a0f7b226e6f74696365223a747275657d",
    ),
];

/// The queue and held-input entries the operations fold keeps in its checkpoints, each holding
/// an unnamed speaker.
fn marked_fold_entries() -> (
    crate::fold::operations::PendingInput,
    crate::fold::operations::Pending,
) {
    let marks = json!({"speaks_for": speaker()});
    (
        crate::fold::operations::PendingInput {
            send_id: None,
            named_speaker: None,
            seq: Some(4),
            placement: Placement::Steer,
            messages: vec![text("steer")],
            written_at_ms: Some(5),
            tags_v1: None,
            submission_id: Some("sub".into()),
            sender: Some("core".into()),
            request_identity_digest: None,
            media: Vec::new(),
            message_marks: Some(marks.clone()),
        },
        crate::fold::operations::Pending {
            send_id: None,
            named_speaker: None,
            submission_id: "sub".into(),
            interrupt: false,
            ordered: true,
            sender: Some("core".into()),
            request_identity_digest: None,
            message_marks: Some(marks),
        },
    )
}

#[test]
fn marked_fold_entries_before_speaker_names_keep_their_bytes() {
    use crate::codec::Message as _;
    let (input, pending) = marked_fold_entries();
    assert_eq!(
        hex(&input.encode()),
        "10042002320d10022209220712057374656572400562037375627204636f7265aa01217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7d"
    );
    assert_eq!(
        hex(&pending.encode()),
        "120373756230014204636f72656a217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7d"
    );
    assert_eq!(
        crate::fold::operations::PendingInput::decode(&input.encode()).unwrap(),
        input
    );
    assert_eq!(
        crate::fold::operations::Pending::decode(&pending.encode()).unwrap(),
        pending
    );
}

/// Freeze the three message shapes to which an optional send id may be added. These bytes were
/// written without a send id and must remain readable and byte-identical when it is unknown.
#[test]
fn send_metadata_before_send_ids_keeps_its_bytes() {
    use crate::codec::Message as _;

    let facts = entry_facts(json!({"speaks_for": speaker()}));
    assert_eq!(
        hex(&facts.encode()),
        "1003200a3002800101a20106676162632e33b2010572756e2d31c00109d201057374656572820204636f72659202217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7df2023822027331320972656d696e6465727342096c696e656167653a34520872656d696e6465726a127b226167656e745f6964223a22416461227d"
    );
    let (input, pending) = marked_fold_entries();
    assert_eq!(
        hex(&input.encode()),
        "10042002320d10022209220712057374656572400562037375627204636f7265aa01217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7d"
    );
    assert_eq!(
        hex(&pending.encode()),
        "120373756230014204636f72656a217b22737065616b735f666f72223a7b226167656e745f6964223a22416461227d7d"
    );
    assert_eq!(EntryFacts::decode(&facts.encode()).unwrap(), facts);
    assert_eq!(
        crate::fold::operations::PendingInput::decode(&input.encode()).unwrap(),
        input
    );
    assert_eq!(
        crate::fold::operations::Pending::decode(&pending.encode()).unwrap(),
        pending
    );
}

#[test]
fn send_id_metadata_is_optional_and_old_decoders_can_skip_it() {
    use crate::codec::{put_opt, skip_unknown, Fields, Message as _};

    let mut facts = entry_facts(json!({"speaks_for": speaker()}));
    let (mut input, mut pending) = marked_fold_entries();
    let old = [facts.encode(), input.encode(), pending.encode()];
    facts.send_id = Some("send-1".into());
    input.send_id = facts.send_id.clone();
    pending.send_id = facts.send_id.clone();
    for ((old, actual), number) in old
        .into_iter()
        .zip([facts.encode(), input.encode(), pending.encode()])
        .zip([25, 12, 8])
    {
        let mut expected = old;
        put_opt(&mut expected, number, false, &Some("send-1".to_owned()));
        assert_eq!(actual, expected);
        let field = Fields::new(&actual, "send metadata")
            .map(Result::unwrap)
            .find(|field| field.number == number)
            .unwrap();
        assert!(!field.must);
        skip_unknown(field, "old send metadata decoder").unwrap();
    }
}

#[test]
fn marked_records_before_speaker_names_keep_their_bytes() {
    let records = marked();
    assert_eq!(records.len(), PINNED.len());
    for (record, (name, pinned)) in records.into_iter().zip(PINNED) {
        assert_eq!(record.name(), name);
        let body = record.encode_body();
        assert_eq!(hex(&body), pinned, "{name}");
        assert_eq!(
            Record::decode(record.kind(), &body).unwrap(),
            record,
            "{name}"
        );
    }
}

/// `value` with its speaker named: the speaker itself, or the speaker of a send's marks.
fn named(mut value: serde_json::Value) -> serde_json::Value {
    if value.get("speaks_for").is_some() {
        value["speaks_for"]["name"] = json!("Ada");
    } else {
        value["name"] = json!("Ada");
    }
    value
}

/// Where each record of [`marked`] keeps the flag of its speaker: the field numbers from the
/// record's body down to the flag.
fn flag_path(record: &Record) -> &'static [u32] {
    match record {
        // The ride envelope (field 7) holds the speaker and its flag (field 6).
        Record::ResultAppend(_) => &[7, 6],
        Record::EntryFacts(_) => &[24],
        Record::RunStart(_) => &[30],
        Record::QueuedSend(_) => &[12],
        Record::Steer(_) => &[7],
        other => panic!("unexpected record {}", other.name()),
    }
}

/// [`marked`] with every speaker named, each beside its flag, as a writer stores it: the name in
/// the JSON, and the flag set.
fn named_records() -> Vec<Record> {
    marked()
        .into_iter()
        .map(|record| match record {
            Record::ResultAppend(mut append) => {
                let ride = append.ride.as_mut().unwrap();
                ride.speaks_for = ride.speaks_for.take().map(named);
                ride.named_speaker = named_speaker(ride.speaks_for.as_ref());
                Record::ResultAppend(append)
            }
            Record::EntryFacts(mut facts) => {
                let facts_mut = std::sync::Arc::make_mut(&mut facts);
                facts_mut.marks = facts_mut
                    .marks
                    .take()
                    .map(|marks| named(marks.to_value()).into());
                facts_mut.named_speaker = marks_name_speaker(facts_mut.marks.as_deref());
                Record::EntryFacts(facts)
            }
            Record::RunStart(mut start) => {
                start.message_marks = start.message_marks.take().map(named);
                start.named_speaker = marks_name_speaker(start.message_marks.as_ref());
                Record::RunStart(start)
            }
            Record::QueuedSend(mut send) => {
                send.message_marks = send.message_marks.take().map(named);
                send.named_speaker = marks_name_speaker(send.message_marks.as_ref());
                Record::QueuedSend(send)
            }
            Record::Steer(mut steer) => {
                steer.message_marks =
                    Some(json!({"speaks_for":{"agent_id":"Ada","name":"Ada"},"notice":true}));
                steer.named_speaker = marks_name_speaker(steer.message_marks.as_ref());
                Record::Steer(steer)
            }
            other => panic!("unexpected record {}", other.name()),
        })
        .collect()
}

/// The must-understand bit of the field `path` names, through the nested messages before its
/// last number, or `None` when the innermost message has no such field.
fn flag_bit(bytes: &[u8], path: &[u32]) -> Option<bool> {
    let (last, outer) = path.split_last().unwrap();
    let inner = outer
        .iter()
        .fold(bytes, |message, number| payload(message, *number));
    must_bit(inner, *last)
}

/// The must-understand bit of field `number` of an encoded message, or `None` when the message
/// has no such field.
fn must_bit(bytes: &[u8], number: u32) -> Option<bool> {
    crate::codec::Fields::new(bytes, "test")
        .map(Result::unwrap)
        .find(|field| field.number == number)
        .map(|field| field.must)
}

fn payload(bytes: &[u8], number: u32) -> &[u8] {
    match crate::codec::Fields::new(bytes, "test")
        .map(Result::unwrap)
        .find(|field| field.number == number)
        .unwrap()
        .raw
    {
        crate::codec::Raw::Len(payload) => payload,
        _ => panic!("field {number} is not a nested message"),
    }
}

#[test]
fn speaker_name_flags_are_set_only_for_a_named_speaker() {
    assert_eq!(named_speaker(None), None);
    assert_eq!(named_speaker(Some(&speaker())), None);
    assert_eq!(named_speaker(Some(&named(speaker()))), Some(true));
    assert_eq!(marks_name_speaker(Some(&json!({"notice": true}))), None);
    assert_eq!(
        marks_name_speaker(Some(&json!({"speaks_for": speaker()}))),
        None
    );
    assert_eq!(
        marks_name_speaker(Some(&named(json!({"speaks_for": speaker()})))),
        Some(true)
    );
}

/// A named speaker is stored beside a must-understand field, so a reader that predates names
/// refuses the record instead of dropping the name; this reader reads the name back.
#[test]
fn named_speakers_round_trip_beside_a_must_understand_field() {
    for record in named_records() {
        let name = record.name();
        let body = record.encode_body();
        assert_eq!(flag_bit(&body, flag_path(&record)), Some(true), "{name}");
        let back = Record::decode(record.kind(), &body).unwrap();
        assert_eq!(back, record, "{name}");
        assert!(
            serde_json::to_string(&back)
                .unwrap()
                .contains("\"name\":\"Ada\""),
            "{name}"
        );
    }
    for record in marked() {
        // Unnamed speakers never write the field.
        let body = record.encode_body();
        assert_eq!(
            flag_bit(&body, flag_path(&record)),
            None,
            "{}",
            record.name()
        );
    }
}

/// The ride envelope's and a ride notice's speakers are flagged in their own messages, nested in
/// the record that holds them.
#[test]
fn nested_named_speakers_carry_their_own_flag() {
    let mut envelope = ride();
    envelope.speaks_for = envelope.speaks_for.take().map(named);
    envelope.named_speaker = named_speaker(envelope.speaks_for.as_ref());
    let append = Record::ResultAppend(ResultAppend {
        send_id: Some("s1".into()),
        run_id: "run-1".into(),
        tool_call_id: ToolCallId("c1".into()),
        text: "reminder".into(),
        notice: None,
        written_at_ms: None,
        ride: Some(envelope),
    });
    let body = append.encode_body();
    assert_eq!(must_bit(payload(&body, 7), 6), Some(true));

    let mut facts = entry_facts(json!({"notice": true}));
    let notice = &mut facts.notices[0];
    notice.speaks_for = notice.speaks_for.take().map(named);
    notice.named_speaker = named_speaker(notice.speaks_for.as_ref());
    let body = Record::EntryFacts(std::sync::Arc::new(facts)).encode_body();
    assert_eq!(must_bit(payload(&body, 23), 7), Some(true));
    // The marks themselves are unnamed, so the message's own flag stays unwritten.
    assert_eq!(must_bit(&body, 24), None);
}

crate::message! {
    /// `Steer` as builds before speaker names declared it.
    #[derive(Debug, Clone, PartialEq)]
    pub struct SteerBeforeNames {
        1 rep input: Message,
        2 opt written_at_ms: u64,
        3 opt tags_v1: serde_json::Value,
        4 opt receipt: serde_json::Value,
        5 rep [must] media: SendMedia,
        6 opt [must] message_marks: serde_json::Value,
    }
}

/// A reader that predates speaker names reads an unnamed steer as before, and refuses a named
/// one by the flag's field number rather than serve the speaker without its name.
#[test]
fn a_reader_before_speaker_names_refuses_a_named_speaker() {
    use crate::codec::{DecodeErrorKind, Message as _};
    let unnamed = marked().pop().unwrap();
    let Record::Steer(unnamed) = unnamed else {
        panic!("the last marked record is a steer")
    };
    let old = SteerBeforeNames::decode(&unnamed.encode()).unwrap();
    assert_eq!(old.message_marks, unnamed.message_marks);
    let named = named_records().pop().unwrap();
    let Record::Steer(named) = named else {
        panic!("the last named record is a steer")
    };
    let refused = SteerBeforeNames::decode(&named.encode()).unwrap_err();
    assert!(
        matches!(refused.kind, DecodeErrorKind::MustUnderstand { field: 7 }),
        "{refused:?}"
    );
}
