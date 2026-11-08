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
        input: vec![user(text)],
        written_at_ms: None,
        tags_v1: None,
        receipt: None,
        media: Vec::new(),
    })
}

fn placed_copy(text: &str, receipt_seq: u64) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: "r".into(),
        placement: Placement::Steer,
        message: user(text),
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
