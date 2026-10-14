use super::*;
use broca_protocol::{ContentBlock, Message, Role};
use broca_wire::{LateApprovalView, SessionReadToolCall};

const THREE_MIB: usize = 3 * 1024 * 1024;

fn message(ordinal: u64, len: usize) -> SessionReadMessage {
    SessionReadMessage {
        ordinal,
        mid: format!("m{ordinal}"),
        message: Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "x".repeat(len),
            }],
        ),
        run: Some(broca_core::RunAttribution {
            run_id: "producing-run".into(),
            episode: "producing-run".into(),
            is_final: false,
        }),
        tool_calls: vec![SessionReadToolCall {
            tool_call_id: "call".into(),
            call_key: Some("lineage:42".into()),
            dispatched_to: Some("tool-provider".into()),
            indeterminate: false,
        }],
    }
}

fn page(messages: Vec<SessionReadMessage>) -> SessionReadResponse {
    let mut page: SessionReadResponse = serde_json::from_value(serde_json::json!({
        "messages": [],
        "head": {"wal_seq": 42, "sub_index": 0},
        "lineage_id": "lineage",
        "lineage_state": {"last_run_id": "latest-run", "state": "completed", "reason": "end_turn"},
        "tools": [{"name": "lookup", "description": "d".repeat(4096), "input_schema": {"type": "object"}}],
        "tools_run_id": "latest-run"
    }))
    .unwrap();
    page.messages = messages;
    page
}

fn late(key: &str, len: usize) -> LateApprovalView {
    LateApprovalView {
        call_key: key.into(),
        elicitation_id: "e".repeat(len),
        decision: "approved".into(),
        outcome: None,
        result: None,
    }
}

#[test]
fn reply_body_cap_includes_envelope_for_tail_and_range() {
    let original = page(vec![
        message(9, THREE_MIB / 2 - 64),
        message(10, THREE_MIB / 2 - 64),
    ]);
    let raw: usize = original
        .messages
        .iter()
        .map(|m| serde_json::to_vec(&m.message).unwrap().len())
        .sum();
    assert!(raw < THREE_MIB, "the old message-only measure fits");
    assert!(serde_json::to_vec(&original).unwrap().len() > THREE_MIB);
    for tail in [false, true] {
        let mut capped = original.clone();
        cap_page_bytes(&mut capped, broca_wire::READ_DEFAULT_MAX_BYTES, tail);
        assert!(serde_json::to_vec(&capped).unwrap().len() < THREE_MIB);
        let kept = usize::from(tail);
        assert_eq!(capped.messages, original.messages[kept..kept + 1]);
        assert_eq!(capped.next_from_ordinal, if tail { None } else { Some(10) });
    }
}

#[test]
fn exact_reply_boundary_includes_changed_range_cursor() {
    let original = page(vec![message(9, 32), message(10, 32), message(11, 32)]);
    let mut expected = original.clone();
    expected.messages.truncate(2);
    expected.next_from_ordinal = Some(11);
    let bytes = serde_json::to_vec(&expected).unwrap().len() as u64;
    let mut fits = original.clone();
    cap_page_bytes(&mut fits, bytes + 1, false);
    assert_eq!(fits, expected);
    let mut boundary = original;
    cap_page_bytes(&mut boundary, bytes, false);
    assert_eq!(boundary.messages.len(), 1);
    assert_eq!(boundary.next_from_ordinal, Some(10));
    assert!(serde_json::to_vec(&boundary).unwrap().len() < bytes as usize);
}

#[test]
fn late_only_reply_counts_envelope_and_continuation_cursor() {
    let mut original = page(Vec::new());
    original.late_approvals = Some(vec![late("l:\"9", 128), late("l:\"10", 128)]);
    let mut expected = original.clone();
    expected.late_approvals.as_mut().unwrap().truncate(1);
    expected.next_late_after = Some("l:\"9".into());
    let bytes = serde_json::to_vec(&expected).unwrap().len() as u64;
    let mut capped = original;
    cap_page_bytes(&mut capped, bytes + 1, false);
    assert_eq!(capped, expected);
    assert!(serde_json::to_vec(&capped).unwrap().len() < (bytes + 1) as usize);
}

#[test]
fn messages_and_late_entries_share_one_reply_budget() {
    let mut original = page(vec![message(0, 128), message(1, 128)]);
    original.late_approvals = Some(vec![late("l:1", 128), late("l:2", 128)]);
    let mut expected = original.clone();
    expected.messages.truncate(1);
    expected.next_from_ordinal = Some(1);
    expected.late_approvals.as_mut().unwrap().truncate(1);
    expected.next_late_after = Some("l:1".into());
    let cap = serde_json::to_vec(&expected).unwrap().len() as u64 + 1;
    let mut capped = original;
    cap_page_bytes(&mut capped, cap, false);
    assert_eq!(capped, expected);
    assert!(serde_json::to_vec(&capped).unwrap().len() < cap as usize);
}

#[test]
fn oversized_atomic_items_and_fixed_metadata_are_not_truncated() {
    let original = page(vec![message(0, THREE_MIB + 1)]);
    let mut capped = original.clone();
    cap_page_bytes(&mut capped, THREE_MIB as u64, false);
    assert_eq!(capped, original);
    assert!(serde_json::to_vec(&capped).unwrap().len() > THREE_MIB);

    let mut original = page(Vec::new());
    original.late_approvals = Some(vec![late("l:1", THREE_MIB + 1)]);
    let mut capped = original.clone();
    cap_page_bytes(&mut capped, THREE_MIB as u64, false);
    assert_eq!(capped, original);

    let original = page(Vec::new());
    let mut capped = original.clone();
    cap_page_bytes(&mut capped, 1, false);
    assert_eq!(capped, original, "unpaged tools and metadata stay whole");
}
